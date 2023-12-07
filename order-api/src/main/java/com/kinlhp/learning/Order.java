package com.kinlhp.learning;

import java.math.BigDecimal;
import java.util.UUID;

public record Order(String customerEmail, UUID orderId, BigDecimal amount) {}
