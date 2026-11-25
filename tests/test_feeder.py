import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import unittest
from unittest import mock

PATH = Path(__file__).resolve().parents[1] / 'repro' / '09_metrics_feeder.py'
spec = importlib.util.spec_from_file_location('feeder', PATH)
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)


class FeederTests(unittest.TestCase):
    def doc(self, side='index', name='topn90d-main-hot', ts=1234, step=1):
        return f.make_doc(side, name, ts, step, 100, 1, 'source:9200')

    def test_node_fields_no_overwrite(self):
        doc = self.doc('node')['payload']['elasticsearch']['node_stats']
        self.assertIn('memory_size_in_bytes', doc['indices']['query_cache'])
        self.assertIn('memory_size_in_bytes', doc['indices']['request_cache'])
        self.assertIn('current_open', doc['http'])
        self.assertNotIn('http', doc['indices'])

    def test_bulk_id_is_cluster_and_side_scoped(self):
        index = self.doc()
        node = self.doc('node')
        other = self.doc()
        other['metadata']['labels']['cluster_id'] = 'another'
        calls = []
        def http(method, url, body=None, **kw):
            calls.append((url, body))
            if '_bulk' in url:
                return 200, json.dumps({'errors': False, 'items': [{'index': {'status': 201}}] * 3})
            return 200, {'_shards': {'failed': 0}}
        with mock.patch.object(f, 'http', side_effect=http):
            self.assertEqual(f.bulk_write([('n', 1, d) for d in (index, node, other)]), 3)
        ids = [json.loads(x)['index']['_id'] for x in calls[0][1].splitlines()[::2]]
        self.assertEqual(len(set(ids)), 3)

    def test_bulk_failure_and_refresh_failure(self):
        docs = [('n', 1, self.doc())]
        with mock.patch.object(f, 'http', return_value=(200, '{"errors": true}')):
            with self.assertRaises(SystemExit): f.bulk_write(docs)
        ok = {'errors': False, 'items': [{'index': {'status': 201}}]}
        with mock.patch.object(f, 'http', side_effect=[(200, ok), (503, {})]):
            with self.assertRaises(SystemExit): f.bulk_write(docs)

    def assert_scope(self, body, side='index'):
        text = json.dumps(body)
        for item in ('metadata.labels.cluster_id', f.CLUSTER, 'metadata.category',
                     'elasticsearch', f.METRIC_NAME[side], f.ENTITY_FIELD[side], 'topn90d-main-'):
            self.assertIn(item, text)

    def test_prune_scoped_and_fail_closed(self):
        with mock.patch.object(f, 'http', return_value=(200, {'count': 2})) as h:
            self.assertEqual(f.do_prune(1, f.prefix_for('main'), True, ('index',)), 2)
            self.assert_scope(h.call_args.args[2])
        with mock.patch.object(f, 'http', return_value=(503, 'no')):
            with self.assertRaises(SystemExit): f.do_prune(1, f.prefix_for('main'))
        with mock.patch.object(f, 'http', return_value=(200, {'deleted': 1, 'failures': [{}]})):
            with self.assertRaises(SystemExit): f.do_prune(1, f.prefix_for('main'))

    def test_state_scoped_latest_single_source_and_failure(self):
        doc = self.doc(ts=3000, step=2)
        response = {'hits': {'total': {'value': 2, 'relation': 'eq'}, 'hits': [{'_source': doc}, {'_source': self.doc(ts=2000, step=99)}]}}
        with mock.patch.object(f, 'ENTITIES', [('hot', 100, 1)]), mock.patch.object(f, 'http', return_value=(200, response)) as h:
            self.assertEqual(f.read_state('main', ('index',))[('index', 'topn90d-main-hot')], {'ts': 3000, 'step': 2})
            self.assert_scope(h.call_args.args[2])
        for response in [(500, {}), (200, {}), (200, {'timed_out': True})]:
            with mock.patch.object(f, 'http', return_value=response):
                with self.assertRaises(SystemExit): f.read_state('main', ('index',))

    def test_status_queries_are_scoped(self):
        first = {'hits': {'total': {'value': 1}}, 'aggregations': {'mn': {'value': 1}, 'mx': {'value': 1}, 'kinds': {'buckets': [{'key': 'index_stats', 'doc_count': 1}]}}}
        second = {'aggregations': {'g': {'buckets': []}}}
        args = mock.Mock(tag='main', side='index')
        with mock.patch.object(f, 'http', side_effect=[(200, first), (200, second)]) as h:
            self.assertEqual(f.cmd_status(args), 0)
            for call in h.call_args_list: self.assert_scope(call.args[2])

    def test_scroll_preserves_names_timestamps_duplicates_and_pages(self):
        docs = [self.doc(ts=1001, step=1), self.doc(ts=1001, step=2), self.doc(ts=1999, step=3)]
        pages = [(200, {'_scroll_id': 'one', 'hits': {'total': {'value': 3, 'relation': 'eq'}, 'hits': [{'_source': docs[0]}]}}),
                 (200, {'_scroll_id': 'two', 'hits': {'hits': [{'_source': d} for d in docs[1:]]}}),
                 (200, {'_scroll_id': 'two', 'hits': {'hits': []}}), (200, {'succeeded': True})]
        out = io.StringIO()
        with mock.patch.object(f, 'http', side_effect=pages) as h:
            n, names = f.resample_side('index', f.prefix_for('main'), 1000, 2000, 60, out)
            self.assert_scope(h.call_args_list[0].args[2])
            self.assertEqual(h.call_args_list[-1].args[0], 'DELETE')
        rows = [json.loads(s) for s in out.getvalue().splitlines()]
        self.assertEqual(n, 3)
        self.assertEqual(names, ['topn90d-main-hot'])
        self.assertEqual([r['ts'] for r in rows], [1001, 1001, 1999])
        self.assertEqual([r['indices'][names[0]]['_synth_step'] for r in rows], [1, 2, 3])

    def test_scroll_node_address_and_incomplete(self):
        doc = self.doc('node')
        start = {'_scroll_id': 's', 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'hits': [{'_source': doc}]}}
        end = {'_scroll_id': 's', 'hits': {'hits': []}}
        out = io.StringIO()
        with mock.patch.object(f, 'http', side_effect=[(200, start), (200, end), (200, {})]):
            f.resample_side('node', f.prefix_for('main'), 1000, 2000, 10, out)
        self.assertEqual(json.loads(out.getvalue())['nodes']['topn90d-main-hot']['transport_address'], 'source:9200')
        start['hits']['total']['value'] = 2
        with mock.patch.object(f, 'http', side_effect=[(200, start), (200, end), (200, {})]):
            with self.assertRaises(SystemExit): f.resample_side('node', f.prefix_for('main'), 1000, 2000, 10, io.StringIO())

    def test_invalid_cli_rejected_before_http(self):
        for argv in (['prune', '--tag', '../bad'], ['prune', '--tag', 'a-b'], ['fill', '--interval', '0'],
                     ['stream', '--hours', 'nan'], ['resample', '--step', '-1'],
                     ['prune', '--hours', '1', '--days', '1']):
            with self.subTest(argv=argv), mock.patch.object(sys, 'argv', ['feeder'] + argv), mock.patch.object(f, 'http') as h, contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit): f.main()
                h.assert_not_called()

    def test_state_same_timestamp_ambiguous_fails(self):
        docs = [self.doc(ts=3000, step=2), self.doc(ts=3000, step=99)]
        response = {'hits': {'total': {'value': 2, 'relation': 'eq'}, 'hits': [{'_source': d} for d in docs]}}
        with mock.patch.object(f, 'ENTITIES', [('hot', 100, 1)]), mock.patch.object(f, 'http', return_value=(200, response)):
            with self.assertRaises(SystemExit): f.read_state('main', ('index',))

    def test_stream_stops_on_state_or_prune_failure(self):
        args = mock.Mock(interval=10, tag='main', side='index', hours=1, days=None,
                         prune_every_hours=1, auto_prune=True)
        with mock.patch.object(f, 'http', return_value=(503, 'error')) as h:
            with self.assertRaises(SystemExit): f.cmd_stream(args)
            self.assertEqual(h.call_count, 1)
        with mock.patch.object(f, 'read_state', return_value={}), mock.patch.object(f, 'http', return_value=(503, 'error')):
            with self.assertRaises(SystemExit): f.cmd_stream(args)

    def test_window_not_aligned_or_shortened(self):
        args = mock.Mock(tag='main', side='index', hours=1, step=60)
        calls = []
        def sample(side, prefix, start, end, step, out):
            calls.append((start, end, step))
            return 0, []
        opener = mock.mock_open()
        with mock.patch.object(f.time, 'time', return_value=12345.678),              mock.patch.object(f.os, 'makedirs'), mock.patch.object(f.os, 'link'),              mock.patch.object(f.os, 'unlink'), mock.patch('builtins.open', opener),              mock.patch.object(f, 'resample_side', side_effect=sample):
            self.assertEqual(f.cmd_resample(args), 0)
        self.assertEqual(calls, [(8745678, 12345678, 60)])

    def test_expected_is_labelled(self):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            f.cmd_expected(mock.Mock(tag='main', side='index', out=None))
        self.assertIn('model-only', out.getvalue())


if __name__ == '__main__':
    unittest.main()
