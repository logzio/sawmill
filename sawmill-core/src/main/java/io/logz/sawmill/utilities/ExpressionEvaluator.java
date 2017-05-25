package io.logz.sawmill.utilities;

import com.google.common.collect.ImmutableMap;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;
import java.util.function.BiFunction;

public class ExpressionEvaluator {
    private final Map<Character, BiFunction<Double,Double,Double>> operationsMap = new ImmutableMap.Builder<Character, BiFunction<Double,Double,Double>>()
            .put('+', (a,b) -> a+b)
            .put('-', (a,b) -> a-b)
            .put('/', (a,b) -> a/b)
            .put('*', (a,b) -> a*b)
            .put('^', (a,b) -> Math.pow(a,b))
            .build();

    public Double calculate(String expression) {
        Queue<Object> rpn =  convertToRPN(expression);
        if (rpn == null || rpn.contains(')') || rpn.contains('(')) return null;
        return calculate(rpn);
    }

    private Double calculate(Queue<Object> rpn) {
        Stack<Double> stack = new Stack<>();
        for (Object item: rpn) {
            if (item instanceof Number) stack.push((Double) item);
            else {
                if (stack.size() < 2) return null;
                double left = stack.pop();
                double right  = stack.pop();

                stack.push(operationsMap.get(item).apply(right,left));
            }
        }
        return stack.pop();
    }

    private Queue<Object> convertToRPN(String expression) {
        Stack<Character> operators = new Stack<>();
        Queue<Object> output = new LinkedList<>();

        Tokenizer tokenizer = new Tokenizer(expression);

        while (tokenizer.hasNext()) {
            Token token = tokenizer.nextToken();
            if (token == null) {
                return null;
            }

            token.getType().run(token.getValue(), operators, output);
        }

        while (!operators.isEmpty()) {
            output.add(operators.pop());
        }

        return output;
    }

    public class Tokenizer {
        private final char[] expression;

        private int pos = 0;

        private Token lastToken;

        public Tokenizer(String expression) {
            this.expression = expression.trim().toCharArray();
        }

        public boolean hasNext() {
            return this.expression.length > pos;
        }

        public Token nextToken() {
            char c = expression[pos];
            while (Character.isWhitespace(c)) {
                c = expression[++pos];
            }

            Token token = null;

            if (operationsMap.keySet().contains(c)) {
                pos++;
                token = new Token(c, TokenType.OPERATION);
            } else if (c == '(') {
                pos++;
                token = new Token(c, TokenType.OPEN_PARENTHESIS);
            } else if (c == ')') {
                pos++;
                token = new Token(c, TokenType.CLOSE_PARENTHESIS);
            } else if (Character.isDigit(c)) {
                int len = 1;
                while (pos + len < expression.length &&
                        (Character.isDigit(expression[pos + len]) || expression[pos + len] == '.')) len++;
                token = new Token(Double.parseDouble(new String(expression, pos, len)), TokenType.NUMBER);
                pos += len;
            }

            boolean invalidOrder = token != null && lastToken != null &&
                    token.getType() == lastToken.getType() &&
                    (token.getType() != TokenType.OPEN_PARENTHESIS &&
                            token.getType() != TokenType.CLOSE_PARENTHESIS);
            if (token == null || invalidOrder) {
                return null;
            }

            lastToken = token;

            return token;
        }
    }

    public class Token {
        private Object value;
        private TokenType type;

        public Token(Object value, TokenType type) {
            this.value = value;
            this.type = type;
        }

        public Object getValue() {
            return value;
        }

        public TokenType getType() {
            return type;
        }
    }

    public enum TokenType {
        NUMBER {
            @Override
            public void run(Object token, Stack<Character> ops, Queue<Object> output) {
                output.add(token);
            }
        },
        OPERATION {
            @Override
            public void run(Object token, Stack<Character> ops, Queue<Object> output) {
                Character c = (Character) token;
                if (ops.isEmpty()) {
                    ops.push(c);
                } else {
                    while (!ops.isEmpty()) {
                        Character peek = ops.peek();
                        if (peek == ')' || peek == '(') break;
                        int prec1 = operationsPrecedence.get(c);
                        int prec2 = operationsPrecedence.get(peek);
                        if (prec2 > prec1 || (prec1 == prec2 && c != '^')) {
                            output.add(ops.pop());
                        } else break;
                    }
                    ops.push(c);
                }
            }
        },
        OPEN_PARENTHESIS {
            @Override
            public void run(Object token, Stack<Character> ops, Queue<Object> output) {
                ops.push((Character) token);
            }
        },
        CLOSE_PARENTHESIS {
            @Override
            public void run(Object token, Stack<Character> ops, Queue<Object> output) {
                while (ops.peek() != '(') {
                    output.add(ops.pop());
                }
                ops.pop();
            }
        };

        public static final Map<Character, Integer> operationsPrecedence = new ImmutableMap.Builder<Character, Integer>()
                .put('^', 4)
                .put('*', 3)
                .put('/', 3)
                .put('+', 2)
                .put('-', 2)
                .build();

        public abstract void run(Object token, Stack<Character> ops, Queue<Object> output);
    }
}
