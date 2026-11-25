package edu.sau.pro3;

import java.math.BigDecimal;
import java.util.List;

public record ExpenseReport(BigDecimal total, long count, List<Expense> expenses) {
    public static final int PAGE_SIZE = 25;

    public record Expense(String occurredAt, String description, String type, int quantity, BigDecimal payment) {}
}
