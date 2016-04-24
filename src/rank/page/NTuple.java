package rank.page;


public class NTuple {
    public class Pair<F, S> {
        private F first; //first member of pair
        private S second; //second member of pair

        public Pair(F first, S second) {
            this.first = first;
            this.second = second;
        }

        public void setFirst(F first) {
            this.first = first;
        }

        public void setSecond(S second) {
            this.second = second;
        }

        public F getFirst() {
            return first;
        }

        public S getSecond() {
            return second;
        }
    }

    public class Triple<F, S, T> {
        private F first; //first member of triple
        private S second; //second member of triple
        private T third; //third member of triple

        public Triple(F first, S second, T third) {
            this.first = first;
            this.second = second;
            this.third = third;
        }

        public void setFirst(F first) {
            this.first = first;
        }

        public void setSecond(S second) {
            this.second = second;
        }

        public void setThird(T third) {
            this.third = third;
        }

        public F getFirst() {
            return first;
        }

        public S getSecond() {
            return second;
        }

        public T getThird() {
            return third;
        }
    }
}
