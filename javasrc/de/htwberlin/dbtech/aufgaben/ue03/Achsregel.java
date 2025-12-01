package de.htwberlin.dbtech.aufgaben.ue03;

final class Achsregel {
    enum Op { EQ, GE }

    private final Op op;
    private final int n;

    private Achsregel(Op op, int n) {
        this.op = op;
        this.n = n;
    }

    static Achsregel parse(String text) {
        if (text == null) return null;
        String s = text.trim();
        try {
            if (s.startsWith("=")) {
                int val = Integer.parseInt(s.substring(1).trim());
                return new Achsregel(Op.EQ, val);
            } else if (s.startsWith(">=")) {
                int val = Integer.parseInt(s.substring(2).trim());
                return new Achsregel(Op.GE, val);
            }
        } catch (NumberFormatException ignore) {
            return null;
        }
        return null;
    }

    boolean matches(int axles) {
        if (op == Op.EQ) return axles == n;
        if (op == Op.GE) return axles >= n;
        return false;
    }
}

