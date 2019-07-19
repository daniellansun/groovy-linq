/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package groovy.linq

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

import java.util.stream.Collectors
import java.util.stream.Stream

import static groovy.linq.Queryable.Order
import static groovy.linq.QueryableCollection.from

@CompileStatic
class QueryableCollectionTest extends GroovyTestCase {
    void testFrom() {
        assert [1, 2, 3] == from(Stream.of(1, 2, 3)).toList()
        assert [1, 2, 3] == from(Arrays.asList(1, 2, 3)).toList()
    }

    void testInnerJoin0() {
        def nums1 = [1, 2, 3]
        def nums2 = [1, 2, 3]
        def result = from(nums1).innerJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, 1], [2, 2], [3, 3]] == result
    }

    void testInnerJoin1() {
        def nums1 = [1, 2, 3]
        def nums2 = [2, 3, 4]
        def result = from(nums1).innerJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[2, 2], [3, 3]] == result
    }

    void testLeftJoin0() {
        def nums1 = [1, 2, 3]
        def nums2 = [1, 2, 3]
        def result = from(nums1).leftJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, 1], [2, 2], [3, 3]] == result
    }

    void testRightJoin0() {
        def nums2 = [1, 2, 3]
        def nums1 = [1, 2, 3]
        def result = from(nums1).rightJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, 1], [2, 2], [3, 3]] == result
    }

    void testLeftJoin1() {
        def nums1 = [1, 2, 3]
        def nums2 = [2, 3, 4]
        def result = from(nums1).leftJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3]] == result
    }

    void testRightJoin1() {
        def nums2 = [1, 2, 3]
        def nums1 = [2, 3, 4]
        def result = from(nums1).rightJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3]] == result
    }

    void testLeftJoin2() {
        def nums1 = [1, 2, 3, null]
        def nums2 = [2, 3, 4]
        def result = from(nums1).leftJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null]] == result
    }

    void testRightJoin2() {
        def nums2 = [1, 2, 3, null]
        def nums1 = [2, 3, 4]
        def result = from(nums1).rightJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null]] == result
    }

    void testLeftJoin3() {
        def nums1 = [1, 2, 3, null]
        def nums2 = [2, 3, 4, null]
        def result = from(nums1).leftJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null]] == result
    }

    void testRightJoin3() {
        def nums2 = [1, 2, 3, null]
        def nums1 = [2, 3, 4, null]
        def result = from(nums1).rightJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null]] == result
    }

    void testLeftJoin4() {
        def nums1 = [1, 2, 3]
        def nums2 = [2, 3, 4, null]
        def result = from(nums1).leftJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3]] == result
    }

    void testRightJoin4() {
        def nums2 = [1, 2, 3]
        def nums1 = [2, 3, 4, null]
        def result = from(nums1).rightJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3]] == result
    }

    void testLeftJoin5() {
        def nums1 = [1, 2, 3, null, null]
        def nums2 = [2, 3, 4]
        def result = from(nums1).leftJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null], [null, null]] == result
    }

    void testRightJoin5() {
        def nums2 = [1, 2, 3, null, null]
        def nums1 = [2, 3, 4]
        def result = from(nums1).rightJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null], [null, null]] == result
    }

    void testLeftJoin6() {
        def nums1 = [1, 2, 3, null, null]
        def nums2 = [2, 3, 4, null]
        def result = from(nums1).leftJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null], [null, null]] == result
    }

    void testRightJoin6() {
        def nums2 = [1, 2, 3, null, null]
        def nums1 = [2, 3, 4, null]
        def result = from(nums1).rightJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null], [null, null]] == result
    }

    void testLeftJoin7() {
        def nums1 = [1, 2, 3, null, null]
        def nums2 = [2, 3, 4, null, null]
        def result = from(nums1).leftJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null], [null, null]] == result
    }

    void testRightJoin7() {
        def nums2 = [1, 2, 3, null, null]
        def nums1 = [2, 3, 4, null, null]
        def result = from(nums1).rightJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null], [null, null]] == result
    }

    void testLeftJoin8() {
        def nums1 = [1, 2, 3, null]
        def nums2 = [2, 3, 4, null, null]
        def result = from(nums1).leftJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null]] == result
    }

    void testRightJoin8() {
        def nums2 = [1, 2, 3, null]
        def nums1 = [2, 3, 4, null, null]
        def result = from(nums1).rightJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3], [null, null]] == result
    }

    void testLeftJoin9() {
        def nums1 = [1, 2, 3]
        def nums2 = [2, 3, 4, null, null]
        def result = from(nums1).leftJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3]] == result
    }

    void testRightJoin9() {
        def nums2 = [1, 2, 3]
        def nums1 = [2, 3, 4, null, null]
        def result = from(nums1).rightJoin(from(nums2), (a, b) -> a == b).toList()
        assert [[1, null], [2, 2], [3, 3]] == result
    }

    void testWhere() {
        def nums = [1, 2, 3, 4, 5]
        def result = from(nums).where(e -> e > 3).toList()
        assert [4, 5] == result
    }

    void testGroupBy() {
        def nums = [1, 2, 2, 3, 3, 4, 4, 5]
        def result = from(nums).groupBy(e -> e, Collectors.counting()).toList()
        assert [[1, 1], [2, 2], [3, 2], [4, 2], [5, 1]] == result
    }

    void testOrderBy() {
        Person daniel = new Person('Daniel', 35)
        Person peter = new Person('Peter', 10)
        Person alice = new Person('Alice', 22)
        Person john = new Person('John', 10)

        def persons = [daniel, peter, alice, john]
        def result = from(persons).orderBy(
                new Order<Person, Comparable>((Person e) -> e.age, true),
                new Order<Person, Comparable>((Person e) -> e.name, true)
        ).toList()
        assert [john, peter, alice, daniel] == result

        result = from(persons).orderBy(
                new Order<Person, Comparable>((Person e) -> e.age, false),
                new Order<Person, Comparable>((Person e) -> e.name, true)
        ).toList()
        assert [daniel, alice, john, peter] == result

        result = from(persons).orderBy(
                new Order<Person, Comparable>((Person e) -> e.age, true),
                new Order<Person, Comparable>((Person e) -> e.name, false)
        ).toList()
        assert [peter, john, alice, daniel] == result

        result = from(persons).orderBy(
                new Order<Person, Comparable>((Person e) -> e.age, false),
                new Order<Person, Comparable>((Person e) -> e.name, false)
        ).toList()
        assert [daniel, alice, peter, john] == result
    }

    void testLimit() {
        def nums = [1, 2, 3, 4, 5]
        def result = from(nums).limit(1, 2).toList()
        assert [2, 3] == result

        result = from(nums).limit(2).toList()
        assert [1, 2] == result
    }

    void testSelect() {
        def nums = [1, 2, 3, 4, 5]
        def result = from(nums).select(e -> e + 1).toList()
        assert [2, 3, 4, 5, 6] == result
    }

    void testFromWhereLimitSelect() {
        def nums1 = [1, 2, 3, 4, 5]
        def nums2 = [0, 1, 2, 3, 4, 5, 6]
        def result =
                from(nums1)
                        .innerJoin(from(nums2), (a, b) -> a == b)
                        .where(t -> t.v1 > 1)
                        .limit(1, 2)
                        .select(t -> t.v1 + 1)
                        .toList()
        assert [4, 5] == result
    }

    @ToString
    @EqualsAndHashCode
    static class Person {
        String name
        int age

        Person(String name, int age) {
            this.name = name
            this.age = age
        }
    }
}
