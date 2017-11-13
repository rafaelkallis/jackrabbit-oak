package com.rafaelkallis.shared;

public class Consumer {

    @FunctionalInterface
    public interface Zero {
        void accept();

        static Zero noOp() {
            return () -> {
            };
        }
    }

    @FunctionalInterface
    public interface One<T1> {
        void accept(T1 arg1);

        static <R> One<R> noOp() {
            return o -> {
            };
        }
    }

    @FunctionalInterface
    public interface Two<T1, T2> {
        void accept(T1 arg1, T2 arg2);

        static <R1, R2> Two<R1, R2> noOp() {
            return (o1, o2) -> {
            };
        }
    }

    @FunctionalInterface
    public interface Three<T1, T2, T3> {
        void accept(T1 arg1, T2 arg2, T3 arg3);

        static <R1, R2, R3> Three<R1, R2, R3> noOp() {
            return (o1, o2, o3) -> {
            };
        }
    }

    @FunctionalInterface
    public interface Four<T1, T2, T3, T4> {
        void accept(T1 arg1, T2 arg2, T3 arg3, T4 arg4);

        static <R1, R2, R3, R4> Four<R1, R2, R3, R4> noOp() {
            return (o1, o2, o3, o4) -> {
            };
        }
    }

    @FunctionalInterface
    public interface Five<T1, T2, T3, T4, T5> {
        void accept(T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5);

        static <R1, R2, R3, R4, R5> Five<R1, R2, R3, R4, R5> noOp() {
            return (o1, o2, o3, o4, o5) -> {
            };
        }
    }

    @FunctionalInterface
    public interface Six<T1, T2, T3, T4, T5, T6> {
        void accept(T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6);

        static <R1, R2, R3, R4, R5, R6> Six<R1, R2, R3, R4, R5, R6> noOp() {
            return (o1, o2, o3, o4, o5, o6) -> {
            };
        }
    }

    @FunctionalInterface
    public interface Seven<T1, T2, T3, T4, T5, T6, T7> {
        void accept(T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7);

        static <R1, R2, R3, R4, R5, R6, R7> Seven<R1, R2, R3, R4, R5, R6, R7> noOp() {
            return (o1, o2, o3, o4, o5, o6, o7) -> {
            };
        }
    }

    @FunctionalInterface
    public interface Eight<T1, T2, T3, T4, T5, T6, T7, T8> {
        void accept(T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7, T8 arg8);

        static <R1, R2, R3, R4, R5, R6, R7, R8> Eight<R1, R2, R3, R4, R5, R6, R7, R8> noOp() {
            return (o1, o2, o3, o4, o5, o6, o7, o8) -> {
            };
        }
    }

    @FunctionalInterface
    public interface Nine<T1, T2, T3, T4, T5, T6, T7, T8, T9> {
        void accept(T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, T6 arg6, T7 arg7, T8 arg8, T9 arg9);

        static <R1, R2, R3, R4, R5, R6, R7, R8, R9> Nine<R1, R2, R3, R4, R5, R6, R7, R8, R9> noOp() {
            return (o1, o2, o3, o4, o5, o6, o7, o8, o9) -> {
            };
        }
    }

}
