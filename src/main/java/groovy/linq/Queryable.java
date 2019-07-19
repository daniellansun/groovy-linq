package groovy.linq;

import groovy.lang.Tuple2;

import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Stream;

public interface Queryable<T> {
    <U> Queryable<Tuple2<T, U>> innerJoin(Queryable<? extends U> queryable, BiPredicate<? super T, ? super U> joiner);
    <U> Queryable<Tuple2<T, U>> leftJoin(Queryable<? extends U> queryable, BiPredicate<? super T, ? super U> joiner);
    <U> Queryable<Tuple2<U, T>> rightJoin(Queryable<? extends U> queryable, BiPredicate<? super U, ? super T> joiner);
    Queryable<T> where(Predicate<? super T> filter);
    <K, R> Queryable<Tuple2<K, R>> groupBy(Function<? super T, ? extends K> classifier, Collector<? super T, ?, R> aggregation);
    <U> Queryable<U> having(Predicate<? super U> filter);
    <U extends Comparable<? super U>> Queryable<T> orderBy(Order<T, U>... orders);
    Queryable<T> limit(int offset, int size);
    default Queryable<T> limit(int size) {
        return limit(0, size);
    }
    <U> Queryable<U> select(Function<? super T, ? extends U> mapper);
    Queryable<T> union(Queryable<? extends T> queryable);
    Queryable<T> unionAll(Queryable<? extends T> queryable);
    Queryable<T> intersect(Queryable<? extends T> queryable);
    Queryable<T> minus(Queryable<? extends T> queryable);
    List<T> toList();
    default Stream<T> stream() {
        return toList().stream();
    }
    // TODO add more methods.

    class Order<T, U extends Comparable<? super U>> {
        private final Function<? super T, ? extends U> keyExtractor;
        private final boolean asc;

        public Order(Function<? super T, ? extends U> keyExtractor, boolean asc) {
            this.keyExtractor = keyExtractor;
            this.asc = asc;
        }

        public Function<? super T, ? extends U> getKeyExtractor() {
            return keyExtractor;
        }

        public boolean isAsc() {
            return asc;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Order)) return false;
            Order<?, ?> order = (Order<?, ?>) o;
            return asc == order.asc &&
                    keyExtractor.equals(order.keyExtractor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(keyExtractor, asc);
        }
    }
}
