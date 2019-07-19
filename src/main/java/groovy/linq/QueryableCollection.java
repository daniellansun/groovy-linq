package groovy.linq;

import groovy.lang.Tuple;
import groovy.lang.Tuple2;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class QueryableCollection<T> implements Queryable<T> {
    private final Iterable<T> sourceIterable;

    public static <T> Queryable<T> from(Iterable<T> sourceIterable) {
        return new QueryableCollection<>(sourceIterable);
    }

    public static <T> Queryable<T> from(Stream<T> sourceStream) {
        return from(sourceStream.collect(Collectors.toList()));
    }

    private QueryableCollection(Iterable<T> sourceIterable) {
        this.sourceIterable = sourceIterable;
    }

    @Override
    public <U> Queryable<Tuple2<T, U>> innerJoin(Queryable<? extends U> queryable, BiPredicate<? super T, ? super U> joiner) {
        Stream<Tuple2<T, U>> stream =
                this.stream()
                        .flatMap(p ->
                                queryable.stream()
                                        .filter(c -> joiner.test(p, c))
                                        .map(c -> Tuple.tuple(p, c)));

        return from(stream);
    }

    @Override
    public <U> Queryable<Tuple2<T, U>> leftJoin(Queryable<? extends U> queryable, BiPredicate<? super T, ? super U> joiner) {
        return outerJoin(this, queryable, joiner);
    }

    @Override
    public <U> Queryable<Tuple2<U, T>> rightJoin(Queryable<? extends U> queryable, BiPredicate<? super U, ? super T> joiner) {
        return outerJoin(queryable, this, joiner);
    }

    @Override
    public Queryable<T> where(Predicate<? super T> filter) {
        Stream<T> stream = this.stream().filter(e -> filter.test(e));

        return from(stream);
    }

    @Override
    public <K, R> Queryable<Tuple2<K, R>> groupBy(Function<? super T, ? extends K> classifier, Collector<? super T, ?, R> aggregation) {
        Stream<Tuple2<K, R>> stream =
                this.stream()
                        .collect(Collectors.groupingBy(classifier, aggregation))
                        .entrySet().stream()
                        .map(e -> Tuple.tuple(e.getKey(), e.getValue()));

        return from(stream);
    }

    @Override
    public <U> Queryable<U> having(Predicate<? super U> filter) {
        return null;
    }

    @Override
    public <U extends Comparable<? super U>> Queryable<T> orderBy(Order<T, U>... orders) {
        Comparator<T> comparator = null;
        for (int i = 0, n = orders.length; i < n; i++) {
            Order<T, U> order = orders[i];
            Comparator<U> ascOrDesc = order.isAsc() ? Comparator.naturalOrder() : Comparator.reverseOrder();
            if (0 == i) {
                comparator = Comparator.comparing(order.getKeyExtractor(), ascOrDesc);
            } else {
                comparator = comparator.thenComparing(order.getKeyExtractor(), ascOrDesc);
            }
        }

        if (null == comparator) {
            return this;
        }

        return from(this.stream().sorted(comparator));
    }

    @Override
    public Queryable<T> limit(int offset, int size) {
        Stream<T> stream = this.stream().skip(offset).limit(size);

        return from(stream);
    }

    @Override
    public <U> Queryable<U> select(Function<? super T, ? extends U> mapper) {
        Stream<U> stream = this.stream().map(mapper);

        return from(stream);
    }

    @Override
    public Queryable<T> union(Queryable<? extends T> queryable) {
        return null;
    }

    @Override
    public Queryable<T> unionAll(Queryable<? extends T> queryable) {
        return null;
    }

    @Override
    public Queryable<T> intersect(Queryable<? extends T> queryable) {
        return null;
    }

    @Override
    public Queryable<T> minus(Queryable<? extends T> queryable) {
        return null;
    }

    @Override
    public List<T> toList() {
        return stream().collect(Collectors.toList());
    }

    @Override
    public Stream<T> stream() {
        return toStream(sourceIterable); // we have to create new stream every time because Java stream can not be reused
    }

    private static <T, U> Queryable<Tuple2<T, U>> outerJoin(Queryable<? extends T> queryable1, Queryable<? extends U> queryable2, BiPredicate<? super T, ? super U> joiner) {
        Stream<Tuple2<T, U>> stream =
                queryable1.stream()
                        .flatMap(p ->
                                queryable2.stream()
                                        .map(c -> joiner.test(p, c) ? c : null)
                                        .reduce(new LinkedList<U>(), (r, e) -> {
                                            int size = r.size();
                                            if (0 == size) {
                                                r.add(e);
                                                return r;
                                            }

                                            int lastIndex = size - 1;
                                            Object lastElement = r.get(lastIndex);

                                            if (null != e) {
                                                if (null == lastElement) {
                                                    r.set(lastIndex, e);
                                                } else {
                                                    r.add(e);
                                                }
                                            }

                                            return r;
                                        }, (i, o) -> o).stream()
                                        .map(c -> null == c ? Tuple.tuple(p, null) : Tuple.tuple(p, c)));

        return from(stream);
    }

    private static <T> Stream<T> toStream(Iterable<T> sourceIterable) {
        return toStream(sourceIterable.iterator());
    }

    private static <T> Stream<T> toStream(Iterator<T> sourceIterator) {
        Iterable<T> iterable = () -> sourceIterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private enum GinqConstant {
        NULL
    }
}
