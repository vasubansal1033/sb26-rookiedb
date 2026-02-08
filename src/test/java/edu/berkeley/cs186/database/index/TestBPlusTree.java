package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.HiddenTests;
import edu.berkeley.cs186.database.categories.Proj2Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.MemoryDiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.recovery.DummyRecoveryManager;
import edu.berkeley.cs186.database.table.RecordId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.*;
import java.util.function.Supplier;

import static org.junit.Assert.*;

@Category(Proj2Tests.class)
public class TestBPlusTree {
    private BufferManager bufferManager;
    private BPlusTreeMetadata metadata;
    private LockContext treeContext;

    // max 5 I/Os per iterator creation default
    private static final int MAX_IO_PER_ITER_CREATE = 5;

    // max 1 I/Os per iterator next, unless overridden
    private static final int MAX_IO_PER_NEXT = 1;

    // 3 seconds max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                3000 * TimeoutScaling.factor)));

    @Before
    public void setup()  {
        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
        diskSpaceManager.allocPart(0);
        this.bufferManager = new BufferManager(diskSpaceManager, new DummyRecoveryManager(), 1024,
                new ClockEvictionPolicy());
        this.treeContext = new DummyLockContext();
        this.metadata = null;
    }

    @After
    public void cleanup() {
        // If you run into errors with this line, try commenting it out and
        // seeing if any other errors appear which may be preventing
        // certain pages from being unpinned correctly.
        this.bufferManager.close();
    }

    // Helpers /////////////////////////////////////////////////////////////////
    private void setBPlusTreeMetadata(Type keySchema, int order) {
        this.metadata = new BPlusTreeMetadata("test", "col", keySchema, order,
                                              0, DiskSpaceManager.INVALID_PAGE_NUM, -1);
    }

    private BPlusTree getBPlusTree(Type keySchema, int order) {
        setBPlusTreeMetadata(keySchema, order);
        return new BPlusTree(bufferManager, metadata, treeContext);
    }

    // the 0th item in maxIOsOverride specifies how many I/Os constructing the iterator may take
    // the i+1th item in maxIOsOverride specifies how many I/Os the ith call to next() may take
    // if there are more items in the iterator than maxIOsOverride, then we default to
    // MAX_IO_PER_ITER_CREATE/MAX_IO_PER_NEXT once we run out of items in maxIOsOverride
    private <T> List<T> indexIteratorToList(Supplier<Iterator<T>> iteratorSupplier,
                                            Iterator<Integer> maxIOsOverride) {
        bufferManager.evictAll();

        long initialIOs = bufferManager.getNumIOs();

        long prevIOs = initialIOs;
        Iterator<T> iter = iteratorSupplier.get();
        long newIOs = bufferManager.getNumIOs();
        long maxIOs = maxIOsOverride.hasNext() ? maxIOsOverride.next() : MAX_IO_PER_ITER_CREATE;
        assertFalse("too many I/Os used constructing iterator (" + (newIOs - prevIOs) + " > " + maxIOs +
                    ") - are you materializing more than you need?",
                    newIOs - prevIOs > maxIOs);

        List<T> xs = new ArrayList<>();
        while (iter.hasNext()) {
            prevIOs = bufferManager.getNumIOs();
            xs.add(iter.next());
            newIOs = bufferManager.getNumIOs();
            maxIOs = maxIOsOverride.hasNext() ? maxIOsOverride.next() : MAX_IO_PER_NEXT;
            assertFalse("too many I/Os used per next() call (" + (newIOs - prevIOs) + " > " + maxIOs +
                        ") - are you materializing more than you need?",
                        newIOs - prevIOs > maxIOs);
        }

        long finalIOs = bufferManager.getNumIOs();
        maxIOs = xs.size() / (2 * metadata.getOrder());
        assertTrue("too few I/Os used overall (" + (finalIOs - initialIOs) + " < " + maxIOs +
                   ") - are you materializing before the iterator is even constructed?",
                   (finalIOs - initialIOs) >= maxIOs);
        return xs;
    }

    private <T> List<T> indexIteratorToList(Supplier<Iterator<T>> iteratorSupplier) {
        return indexIteratorToList(iteratorSupplier, Collections.emptyIterator());
    }

    // Tests ///////////////////////////////////////////////////////////////////

    @Test
    @Category(PublicTests.class)
    public void testSimpleBulkLoad() {
        // Creates a B+ Tree with order 2, fillFactor 0.75 and attempts to bulk
        // load 11 values.

        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        float fillFactor = 0.75f;
        assertEquals("()", tree.toSexp());

        List<Pair<DataBox, RecordId>> data = new ArrayList<>();
        for (int i = 1; i <= 11; ++i) {
            data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
        }

        tree.bulkLoad(data.iterator(), fillFactor);
        //      (    4        7         10        _   )
        //       /       |         |         \
        // (1 2 3 _) (4 5 6 _) (7 8 9 _) (10 11 _ _)
        String leaf0 = "((1 (1 1)) (2 (2 2)) (3 (3 3)))";
        String leaf1 = "((4 (4 4)) (5 (5 5)) (6 (6 6)))";
        String leaf2 = "((7 (7 7)) (8 (8 8)) (9 (9 9)))";
        String leaf3 = "((10 (10 10)) (11 (11 11)))";
        String sexp = String.format("(%s 4 %s 7 %s 10 %s)", leaf0, leaf1, leaf2, leaf3);
        assertEquals(sexp, tree.toSexp());
    }

    @Test
    @Category(PublicTests.class)
    public void testWhiteBoxTest() {
        // This test will insert values one by one into your B+ tree implementation.
        // We've provided a visualization of how your tree should be structured
        // after each step.
        BPlusTree tree = getBPlusTree(Type.intType(), 1);
        assertEquals("()", tree.toSexp());

        // (4)
        tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
        assertEquals("((4 (4 4)))", tree.toSexp());

        // (4 9)
        tree.put(new IntDataBox(9), new RecordId(9, (short) 9));
        assertEquals("((4 (4 4)) (9 (9 9)))", tree.toSexp());

        //   (6)
        //  /   \
        // (4) (6 9)
        tree.put(new IntDataBox(6), new RecordId(6, (short) 6));
        String l = "((4 (4 4)))";
        String r = "((6 (6 6)) (9 (9 9)))";
        assertEquals(String.format("(%s 6 %s)", l, r), tree.toSexp());

        //     (6)
        //    /   \
        // (2 4) (6 9)
        tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
        l = "((2 (2 2)) (4 (4 4)))";
        r = "((6 (6 6)) (9 (9 9)))";
        assertEquals(String.format("(%s 6 %s)", l, r), tree.toSexp());

        //      (6 7)
        //     /  |  \
        // (2 4) (6) (7 9)
        tree.put(new IntDataBox(7), new RecordId(7, (short) 7));
        l = "((2 (2 2)) (4 (4 4)))";
        String m = "((6 (6 6)))";
        r = "((7 (7 7)) (9 (9 9)))";
        assertEquals(String.format("(%s 6 %s 7 %s)", l, m, r), tree.toSexp());

        //         (7)
        //        /   \
        //     (6)     (8)
        //    /   \   /   \
        // (2 4) (6) (7) (8 9)
        tree.put(new IntDataBox(8), new RecordId(8, (short) 8));
        String ll = "((2 (2 2)) (4 (4 4)))";
        String lr = "((6 (6 6)))";
        String rl = "((7 (7 7)))";
        String rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 6 %s)", ll, lr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 7 %s)", l, r), tree.toSexp());

        //            (7)
        //           /   \
        //     (3 6)       (8)
        //   /   |   \    /   \
        // (2) (3 4) (6) (7) (8 9)
        tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
        ll = "((2 (2 2)))";
        String lm = "((3 (3 3)) (4 (4 4)))";
        lr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s 6 %s)", ll, lm, lr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 7 %s)", l, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //   (3)      (6)       (8)
        //  /   \    /   \    /   \
        // (2) (3) (4 5) (6) (7) (8 9)
        tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        String ml = "((4 (4 4)) (5 (5 5)))";
        String mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (1 2) (3) (4 5) (6) (7) (8 9)
        tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
        ll = "((1 (1 1)) (2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (4 5) (6) (7) (8 9)
        tree.remove(new IntDataBox(1));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (4 5) (6) (7) (8  )
        tree.remove(new IntDataBox(9));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (4 5) ( ) (7) (8  )
        tree.remove(new IntDataBox(6));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (  5) ( ) (7) (8  )
        tree.remove(new IntDataBox(4));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((5 (5 5)))";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) (3) (  5) ( ) (7) (8  )
        tree.remove(new IntDataBox(2));
        ll = "()";
        lr = "((3 (3 3)))";
        ml = "((5 (5 5)))";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) (3) (   ) ( ) (7) (8  )
        tree.remove(new IntDataBox(5));
        ll = "()";
        lr = "((3 (3 3)))";
        ml = "()";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) (3) (   ) ( ) ( ) (8  )
        tree.remove(new IntDataBox(7));
        ll = "()";
        lr = "((3 (3 3)))";
        ml = "()";
        mr = "()";
        rl = "()";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) ( ) (   ) ( ) ( ) (8  )
        tree.remove(new IntDataBox(3));
        ll = "()";
        lr = "()";
        ml = "()";
        mr = "()";
        rl = "()";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) ( ) (   ) ( ) ( ) (   )
        tree.remove(new IntDataBox(8));
        ll = "()";
        lr = "()";
        ml = "()";
        mr = "()";
        rl = "()";
        rr = "()";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());
    }

    @Test
    @Category(PublicTests.class)
    public void testRandomPuts() {
        // This test will generate 1000 keys and for trees of degree 2, 3 and 4
        // will scramble the keys and attempt to insert them.
        //
        // After insertion we test scanAll and scanGreaterEqual to ensure all
        // the keys were inserted and could be retrieved in the proper order.
        //
        // Finally, we remove each of the keys one-by-one and check to see that
        // they can no longer be retrieved.

        List<DataBox> keys = new ArrayList<>();
        List<RecordId> rids = new ArrayList<>();
        List<RecordId> sortedRids = new ArrayList<>();
        for (int i = 0; i < 1000; ++i) {
            keys.add(new IntDataBox(i));
            rids.add(new RecordId(i, (short) i));
            sortedRids.add(new RecordId(i, (short) i));
        }

        // Try trees with different orders.
        for (int d = 2; d < 5; ++d) {
            // Try trees with different insertion orders.
            for (int n = 0; n < 2; ++n) {
                Collections.shuffle(keys, new Random(42));
                Collections.shuffle(rids, new Random(42));

                // Insert all the keys.
                BPlusTree tree = getBPlusTree(Type.intType(), d);
                for (int i = 0; i < keys.size(); ++i) {
                    tree.put(keys.get(i), rids.get(i));
                }

                // Test get.
                for (int i = 0; i < keys.size(); ++i) {
                    assertEquals(Optional.of(rids.get(i)), tree.get(keys.get(i)));
                }

                // Test scanAll.
                assertEquals(sortedRids, indexIteratorToList(tree::scanAll));

                // Test scanGreaterEqual.
                for (int i = 0; i < keys.size(); i += 100) {
                    final int j = i;
                    List<RecordId> expected = sortedRids.subList(i, sortedRids.size());
                    assertEquals(expected, indexIteratorToList(() -> tree.scanGreaterEqual(new IntDataBox(j))));
                }

                // Load the tree from disk.
                BPlusTree fromDisk = new BPlusTree(bufferManager, metadata, treeContext);
                assertEquals(sortedRids, indexIteratorToList(fromDisk::scanAll));

                // Test remove.
                Collections.shuffle(keys, new Random(42));
                Collections.shuffle(rids, new Random(42));
                for (DataBox key : keys) {
                    fromDisk.remove(key);
                    assertEquals(Optional.empty(), fromDisk.get(key));
                }
            }
        }
    }

    // ========== Unit tests for get/put/scans/iterator/bulkLoad (recent commits) ==========

    @Test
    @Category(PublicTests.class)
    public void testGetEmptyTree() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        assertEquals(Optional.empty(), tree.get(new IntDataBox(0)));
        assertEquals(Optional.empty(), tree.get(new IntDataBox(100)));
    }

    @Test
    @Category(PublicTests.class)
    public void testScanAllEmptyTree() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        List<RecordId> result = indexIteratorToList(tree::scanAll);
        assertTrue(result.isEmpty());
    }

    @Test
    @Category(PublicTests.class)
    public void testScanGreaterEqualEmptyTree() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        List<RecordId> result = indexIteratorToList(() -> tree.scanGreaterEqual(new IntDataBox(0)));
        assertTrue(result.isEmpty());
    }

    @Test
    @Category(PublicTests.class)
    public void testScanGreaterEqualKeyLargerThanAll() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
        tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
        tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
        List<RecordId> result = indexIteratorToList(() -> tree.scanGreaterEqual(new IntDataBox(10)));
        assertTrue(result.isEmpty());
    }

    @Test
    @Category(PublicTests.class)
    public void testScanGreaterEqualKeySmallerThanAll() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        List<RecordId> expected = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            tree.put(new IntDataBox(i), new RecordId(i, (short) i));
            expected.add(new RecordId(i, (short) i));
        }
        List<RecordId> result = indexIteratorToList(() -> tree.scanGreaterEqual(new IntDataBox(0)));
        assertEquals(expected, result);
    }

    @Test
    @Category(PublicTests.class)
    public void testScanGreaterEqualExactKey() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
        tree.put(new IntDataBox(10), new RecordId(10, (short) 10));
        tree.put(new IntDataBox(15), new RecordId(15, (short) 15));
        List<RecordId> result = indexIteratorToList(() -> tree.scanGreaterEqual(new IntDataBox(10)));
        assertEquals(Arrays.asList(new RecordId(10, (short) 10), new RecordId(15, (short) 15)), result);
    }

    @Test
    @Category(PublicTests.class)
    public void testIteratorAcrossMultipleLeaves() {
        // Order 2 -> max 4 keys per leaf. Insert enough to force multiple leaves.
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        List<RecordId> expected = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            tree.put(new IntDataBox(i), new RecordId(i, (short) i));
            expected.add(new RecordId(i, (short) i));
        }
        assertEquals(expected, indexIteratorToList(tree::scanAll));
        // scanGreaterEqual from middle
        List<RecordId> fromFive = indexIteratorToList(() -> tree.scanGreaterEqual(new IntDataBox(5)));
        assertEquals(expected.subList(5, 12), fromFive);
    }

    @Test
    @Category(PublicTests.class)
    public void testPutCausesRootSplitThenGetAndScan() {
        BPlusTree tree = getBPlusTree(Type.intType(), 1);
        // With order 1, a few inserts will split and create new root.
        for (int i = 0; i < 8; i++) {
            tree.put(new IntDataBox(i), new RecordId(i, (short) i));
        }
        for (int i = 0; i < 8; i++) {
            assertEquals(Optional.of(new RecordId(i, (short) i)), tree.get(new IntDataBox(i)));
        }
        List<RecordId> all = indexIteratorToList(tree::scanAll);
        assertEquals(8, all.size());
        for (int i = 0; i < 8; i++) {
            assertEquals(new RecordId(i, (short) i), all.get(i));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testRemoveThenGet() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        tree.put(new IntDataBox(7), new RecordId(7, (short) 7));
        assertEquals(Optional.of(new RecordId(7, (short) 7)), tree.get(new IntDataBox(7)));
        tree.remove(new IntDataBox(7));
        assertEquals(Optional.empty(), tree.get(new IntDataBox(7)));
    }

    @Test
    @Category(PublicTests.class)
    public void testLoadFromDiskAfterPuts() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        List<RecordId> expected = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            tree.put(new IntDataBox(i), new RecordId(i, (short) i));
            expected.add(new RecordId(i, (short) i));
        }
        BPlusTree fromDisk = new BPlusTree(bufferManager, metadata, treeContext);
        assertEquals(expected, indexIteratorToList(fromDisk::scanAll));
        for (int i = 0; i < 20; i++) {
            assertEquals(Optional.of(new RecordId(i, (short) i)), fromDisk.get(new IntDataBox(i)));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testBulkLoadThenScanAndGet() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        float fillFactor = 0.75f;
        List<Pair<DataBox, RecordId>> data = new ArrayList<>();
        List<RecordId> expected = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
            expected.add(new RecordId(i, (short) i));
        }
        tree.bulkLoad(data.iterator(), fillFactor);
        assertEquals(expected, indexIteratorToList(tree::scanAll));
        for (int i = 0; i < 15; i++) {
            assertEquals(Optional.of(new RecordId(i, (short) i)), tree.get(new IntDataBox(i)));
        }
        List<RecordId> fromSeven = indexIteratorToList(() -> tree.scanGreaterEqual(new IntDataBox(7)));
        assertEquals(expected.subList(7, 15), fromSeven);
    }

    @Test
    @Category(PublicTests.class)
    public void testBulkLoadThenLoadFromDisk() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        List<Pair<DataBox, RecordId>> data = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
        }
        tree.bulkLoad(data.iterator(), 0.75f);
        BPlusTree fromDisk = new BPlusTree(bufferManager, metadata, treeContext);
        List<RecordId> expected = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            expected.add(new RecordId(i, (short) i));
        }
        assertEquals(expected, indexIteratorToList(fromDisk::scanAll));
    }

    @Test(expected = BPlusTreeException.class)
    @Category(PublicTests.class)
    public void testBulkLoadNonEmptyTreeThrows() {
        // Implementation throws only when root is an InnerNode (root != root.getLeftmostLeaf()).
        // With order 2, a leaf holds at most 4 keys; 5th insert causes root to become InnerNode.
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        for (int i = 0; i < 5; i++) {
            tree.put(new IntDataBox(i), new RecordId(i, (short) i));
        }
        List<Pair<DataBox, RecordId>> data = new ArrayList<>();
        data.add(new Pair<>(new IntDataBox(10), new RecordId(10, (short) 10)));
        tree.bulkLoad(data.iterator(), 0.5f);
    }

    @Test
    @Category(PublicTests.class)
    public void testScanGreaterEqualBoundaryBetweenLeaves() {
        // Order 2: fill one leaf (e.g. 4 keys), then add more so we have 2+ leaves.
        // Query with key equal to first key of second leaf.
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        for (int i = 0; i < 8; i++) {
            tree.put(new IntDataBox(i), new RecordId(i, (short) i));
        }
        List<RecordId> fromFour = indexIteratorToList(() -> tree.scanGreaterEqual(new IntDataBox(4)));
        assertEquals(4, fromFour.size());
        assertEquals(new RecordId(4, (short) 4), fromFour.get(0));
        assertEquals(new RecordId(7, (short) 7), fromFour.get(3));
    }

    @Test
    @Category(PublicTests.class)
    public void testGetNonexistentKeyInNonEmptyTree() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
        tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
        assertEquals(Optional.empty(), tree.get(new IntDataBox(0)));
        assertEquals(Optional.empty(), tree.get(new IntDataBox(2)));
        assertEquals(Optional.empty(), tree.get(new IntDataBox(5)));
    }

    @Test
    @Category(PublicTests.class)
    public void testRemoveNonexistentKeyNoOp() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
        tree.remove(new IntDataBox(2)); // not present
        assertEquals(Optional.of(new RecordId(1, (short) 1)), tree.get(new IntDataBox(1)));
    }

    @Test
    @Category(SystemTests.class)
    public void testMaxOrder() {
        // Note that this white box test depend critically on the implementation
        // of toBytes and includes a lot of magic numbers that won't make sense
        // unless you read toBytes.
        assertEquals(4, Type.intType().getSizeInBytes());
        assertEquals(8, Type.longType().getSizeInBytes());
        assertEquals(10, RecordId.getSizeInBytes());
        short pageSizeInBytes = 100;
        Type keySchema = Type.intType();
        assertEquals(3, LeafNode.maxOrder(pageSizeInBytes, keySchema));
        assertEquals(3, InnerNode.maxOrder(pageSizeInBytes, keySchema));
        assertEquals(3, BPlusTree.maxOrder(pageSizeInBytes, keySchema));
    }
}
