import os
import random
import threading
import time
import pytest
import logging

from cassandra import ConsistencyLevel

from dtest import Tester, create_ks
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('3.0')
class TestHintedHandoffConfig(Tester):
    """
    Tests the hinted handoff configuration options introduced in
    CASSANDRA-9035.

    @jira_ticket CASSANDRA-9035
    """
    def create_ddl(self, session, rf={'dc1': 3, 'dc2': 3}):
        create_ks(session, 'orphan_hint_files', rf)
        session.execute('CREATE TABLE test (id uuid PRIMARY KEY, val1 text, group text)')

    def _start_two_node_cluster(self, config_options=None):
        """
        Start a cluster with two nodes and return them
        """
        cluster = self.cluster

        if config_options:
            cluster.set_configuration_options(values=config_options)

        if not self.dtest_config.use_vnodes:
            cluster.populate([2]).start()
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate([2], tokens=tokens).start()

        return cluster.nodelist()

    def _launch_nodetool_cmd(self, node, cmd):
        """
        Launch a nodetool command and check there is no error, return the result
        """
        out, err, _ = node.nodetool(cmd)
        assert '' == err
        return out

    def _do_hinted_handoff(self, node1, node2, enabled, keyspace='ks'):
        """
        Test that if we stop one node the other one
        will store hints only when hinted handoff is enabled
        """
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, keyspace, 2)
        create_c1c2_table(self, session)

        node2.stop(wait_other_notice=True)

        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ONE)

        log_mark = node1.mark_log()
        node2.start(wait_other_notice=True)

        if enabled:
            node1.watch_log_for(["Finished hinted"], from_mark=log_mark, timeout=120)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been delivered via HH if enabled or not if not enabled
        session = self.patient_exclusive_cql_connection(node2, keyspace=keyspace)
        for n in range(0, 100):
            if enabled:
                query_c1c2(session, n, ConsistencyLevel.ONE)
            else:
                query_c1c2(session, n, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)

    def test_nodetool(self):
        """
        Test various nodetool commands
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

            self._launch_nodetool_cmd(node, 'disablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is not running' == res.rstrip()

            self._launch_nodetool_cmd(node, 'enablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

            self._launch_nodetool_cmd(node, 'disablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep) == res.rstrip()

            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

    def test_hintedhandoff_disabled(self):
        """
        Test gloabl hinted handoff disabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': False})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is not running' == res.rstrip()

        self._do_hinted_handoff(node1, node2, False)

    def test_hintedhandoff_enabled(self):
        """
        Test global hinted handoff enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

        self._do_hinted_handoff(node1, node2, True)

    @since('4.0')
    def test_hintedhandoff_setmaxwindow(self):
        """
        Test global hinted handoff against max_hint_window_in_ms update via nodetool
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True, "max_hint_window_in_ms": 300000})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

        res = self._launch_nodetool_cmd(node, 'getmaxhintwindow')
        assert 'Current max hint window: 300000 ms' == res.rstrip()
        self._do_hinted_handoff(node1, node2, True)
        node1.start(wait_other_notice=True)
        self._launch_nodetool_cmd(node, 'setmaxhintwindow 1')
        res = self._launch_nodetool_cmd(node, 'getmaxhintwindow')
        assert 'Current max hint window: 1 ms' == res.rstrip()
        self._do_hinted_handoff(node1, node2, False, keyspace='ks2')

    def test_hintedhandoff_dc_disabled(self):
        """
        Test global hinted handoff enabled with the dc disabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True,
                                                     'hinted_handoff_disabled_datacenters': ['dc1']})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep) == res.rstrip()

        self._do_hinted_handoff(node1, node2, False)

    def test_hintedhandoff_dc_reenabled(self):
        """
        Test global hinted handoff enabled with the dc disabled first and then re-enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True,
                                                     'hinted_handoff_disabled_datacenters': ['dc1']})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep) == res.rstrip()

        for node in node1, node2:
            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

        self._do_hinted_handoff(node1, node2, True)

    @since('3.0')
    def test_orphan_hint_files(self):
        condition = threading.Event()
        cluster = self.cluster

        # throttle hinted handoff so that we can perform different steps like removenode, etc.
        # while hint delivery is in-progress
        request_timeout_ms = 2000
        write_timeout_ms = 5000
        cluster.set_configuration_options(values={'request_timeout_in_ms': request_timeout_ms,
                                                  'hinted_handoff_throttle_in_kb': 100,
                                                  'write_request_timeout_in_ms': write_timeout_ms})


        #minrpctimeout + writerpctimeout
        hint_files_purge_max_delay_in_sec = (request_timeout_ms + write_timeout_ms)/1000

        cluster.populate([3, 3]).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]
        node2 = cluster.nodelist()[1]

        session = self.patient_cql_connection(node2)
        session.consistency_level = 'LOCAL_QUORUM'

        self.create_ddl(session)

        class ThreadedQuery(threading.Thread):

            def __init__(self, connection, condition):
                threading.Thread.__init__(self)
                self.connection = connection
                self.continue_traffic = True
                self.condition = condition

            def run(self):
                session = self.connection
                j = 0
                try:
                    while self.continue_traffic:
                        session.execute("INSERT INTO orphan_hint_files.test (id, val1, group) VALUES (%s, '%s', '%s')" % (str(uuid.uuid1()), "ABC", "PQRST"))
                        j = j + 1
                        if (j > 10000):
                            self.condition.set()
                except Exception:
                    self.condition.set()

            def stop_traffic(self):
                self.continue_traffic = False

        threads = []
        for x in range(20):
            conn = self.cql_connection(random.choice(cluster.nodelist()))
            threads.append(ThreadedQuery(conn, condition))

        # stop node for a while to let hints built up
        node1.nodetool('disablebinary')
        node1.nodetool('disablegossip')

        for t in threads:
            t.start()

        # wait for threads to insert at-least some data
        condition.wait()

        node1.nodetool('enablegossip')
        node1.nodetool('enablebinary')

        time.sleep(1)
        # start node so hints get propagated
        node1.nodetool('stopdaemon')

        # now remove node from the cluster
        node2.nodetool('assassinate 127.0.0.1')

        for t in threads:
            t.stop_traffic()

        for t in threads:
            t.join()

        # wait until hint thread removes files for the just assassinated node
        time.sleep(hint_files_purge_max_delay_in_sec)

        #verify that there are no orphan hint files present
        for node in cluster.nodelist():
            for name in os.listdir(os.path.join(node.get_path(), 'hints')):
                self.fail("Hints should not be present after we assassinated a node {}".format(name))

class TestHintedHandoff(Tester):

    @pytest.mark.no_vnodes
    def test_hintedhandoff_decom(self):
        self.cluster.populate(4).start(wait_for_binary_proto=True)
        [node1, node2, node3, node4] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_c1c2_table(self, session)
        node4.stop(wait_other_notice=True)
        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ONE)
        node1.decommission()
        node4.start(wait_for_binary_proto=True)

        force = True if self.cluster.version() >= '3.12' else False
        node2.decommission(force=force)
        node3.decommission(force=force)

        time.sleep(5)
        for x in range(0, 100):
            query_c1c2(session, x, ConsistencyLevel.ONE)
