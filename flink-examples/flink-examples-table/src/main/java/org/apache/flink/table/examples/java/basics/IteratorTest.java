package org.apache.flink.table.examples.java.basics;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * @author haxi
 * @description
 * @date 2024/4/11 19:21
 */
public class IteratorTest {
    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        list.add(0);
        list.add(1);
        list.add(2);
        list.add(3);
//        list.add(4);
        ListIterator<Integer> iter = list.listIterator();
        while (iter.hasNext()) {
            if (iter.next() >= 3) {
//                System.out.println(iter.nextIndex());
                System.out.println(iter.previousIndex());
            }
        }
    }
}
