package com.example.api_gateway.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.core.Ordered;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.Base64;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
// @ConditionalOnProperty(value = "app.kafka.enabled", havingValue = "true")
public class RequestEventLogFilter implements GlobalFilter, Ordered {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final String TOPIC = "api-gateway-event-log";
    private static final int LOG_RETENTION_MINUTES = 20; // Set to expire after 20 minutes

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        long start = System.currentTimeMillis();
        ServerHttpRequest request = exchange.getRequest();
        String path = request.getPath().value();

        return chain.filter(exchange)
                .doFinally(signalType -> {
                    try {
                        long latency = System.currentTimeMillis() - start;
                        String method = request.getMethod() != null ? request.getMethod().name() : "";
                        String uri = request.getURI().toString();
                        String ip = request.getRemoteAddress() != null
                                ? request.getRemoteAddress().getAddress().getHostAddress() : "";
                        String operatorId = request.getHeaders().getFirst("X-Operator-Id");
                        String correlationId = request.getHeaders().getFirst("X-Correlation-Id");
                        int status = exchange.getResponse().getStatusCode() != null
                                ? exchange.getResponse().getStatusCode().value() : 0;

                        // Determine service and initial classification
                        String serviceName = determineServiceName(path);
                        String logType = "ACCESS";  // Default là ACCESS khi request mới vào
                        String action = method + "_REQUEST"; // default
                        
                        // Check service availability first
                        if (path.startsWith("/api/users")) {
                            serviceName = "user-service";
                            if (!isServiceAvailable("user-service")) {
                                logType = "ERROR";
                                action = "SERVICE_UNAVAILABLE";
                                status = 503;
                            }
                        } else if (path.startsWith("/api/monitoring")) {
                            serviceName = "monitoring-service";
                            if (!isServiceAvailable("monitoring-service")) {
                                logType = "ERROR";
                                action = "SERVICE_UNAVAILABLE";
                                status = 503;
                            }
                        }
                        
                        // Get JWT token if exists
                        String jwt = request.getHeaders().getFirst("Authorization");
                        String username = extractUsernameFromJwt(jwt);
                        
                        // Then apply final classification based on status
                        if (status >= 400) {
                            logType = "ERROR";
                            action = status >= 500 ? "SERVER_ERROR_" + status : "CLIENT_ERROR_" + status;
                        } else if (status >= 200 && status < 300) {
                            // Nếu request thành công (2xx), đánh dấu là GATEWAY
                            logType = "GATEWAY";
                            // Xác định action dựa trên path
                            if (path.contains("/auth") || path.contains("/login") || path.contains("/logout")) {
                                action = method + "_" + (path.contains("/login") ? "LOGIN" : 
                                        path.contains("/logout") ? "LOGOUT" : "AUTH");
                            } else if (method.equals("POST") && path.endsWith("/users")) {
                                action = "USER_CREATED";
                            }
                        }

                        // Build payload with standardized structure
                        Map<String, Object> payload = new HashMap<>();
                        payload.put("method", method);
                        payload.put("path", uri);
                        payload.put("ip", ip);
                        payload.put("status", status);
                        payload.put("statusCategory", status / 100 + "xx");
                        payload.put("latencyMs", latency);
                        payload.put("backendService", serviceName);
                        payload.put("requestHeaders", getHeadersAsMap(request));
                        
                                // Add context-specific information
                        // Add performance metrics for fast responses
                        if (latency < 100) {
                            payload.put("isFastResponse", true);
                            payload.put("thresholdMs", 100);
                            payload.put("actualLatencyMs", latency);
                            payload.put("performanceGain", 100 - latency);
                        }
                        
                        // Add security info for auth-related requests
                        if (path.contains("/auth") || path.contains("/login") || path.contains("/logout")) {
                            payload.put("authType", jwt != null ? "JWT" : "NONE");
                            payload.put("authenticated", jwt != null);
                            payload.put("username", extractUsernameFromJwt(jwt));
                        }
                        
                        // Add error details for error responses
                        if (status >= 400) {
                            payload.put("errorCode", status);
                            payload.put("errorCategory", "Server Error");
                            payload.put("errorMessage", HttpStatus.valueOf(status).getReasonPhrase());
                        }

                        // Generate event ID and traceId
                        String eventId = generateEventId(serviceName);
                        String traceId = UUID.randomUUID().toString();
                        exchange.getResponse().getHeaders().add("X-Trace-Id", traceId);

                        // Generate appropriate message based on event type
                        String message;
                        switch (logType) {
                            case "ERROR":
                                message = String.format("Error occurred in %s: %s", serviceName, action);
                                break;
                            case "SECURITY":
                                message = String.format("Security event: %s from %s", action, ip);
                                break;
                            case "PERFORMANCE":
                                message = String.format("Slow response detected in %s: %dms", serviceName, latency);
                                break;
                            case "BUSINESS":
                                message = String.format("Business operation %s completed in %s", action, serviceName);
                                break;
                            default:
                                message = String.format("%s request to %s", method, serviceName);
                        }

                        // Build comprehensive event matching EventLogListener structure
                        Map<String, Object> event = new HashMap<>();
                        event.put("eventId", eventId);
                        event.put("logType", logType);
                        event.put("action", action);
                        event.put("serviceName", serviceName);
                        event.put("traceId", traceId);
                        
                        // Ensure operator information is never null
                        event.put("operatorId", operatorId != null ? operatorId : "SYSTEM");
                        event.put("operatorName", username != null ? username : "SYSTEM");
                        
                        // Use traceId as fallback for correlationId
                        event.put("correlationId", correlationId != null ? correlationId : traceId);
                        
                        event.put("severity", determineSeverity(status, latency));
                        event.put("eventTimestamp", Instant.ofEpochMilli(start).toString());
                        event.put("createdAt", Instant.now().toString());
                        event.put("message", message != null ? message : action); // Use action as fallback message
                        
                        // Set expireAt for log retention based on eventTimestamp
                        Instant eventTimestampInstant = Instant.ofEpochMilli(start);
                        Instant expireAt = eventTimestampInstant.plus(30, ChronoUnit.DAYS);
                        event.put("expireAt", expireAt.toString());
                        
                        event.put("payload", payload);
                        
                        // Ensure tags are never null
                        List<String> tags = generateTags(logType, status, latency);
                        if (tags == null || tags.isEmpty()) {
                            tags = new ArrayList<>();
                            tags.add(logType.toLowerCase());
                        }
                        event.put("tags", tags);

                        String logJson = objectMapper.writeValueAsString(event);
                        kafkaTemplate.send(TOPIC, logJson);

                        log.info("{} log sent to Kafka: service={}, path={}",
                                logType, serviceName, uri);
                    } catch (Exception e) {
                        log.error("Failed to send log to Kafka", e);
                        try {
                            // Create proper error event with same structure
                            Map<String, Object> errorEvent = new HashMap<>();
                            String errorEventId = generateEventId(determineServiceName(request.getPath().value()));
                            
                            String currentServiceName = determineServiceName(request.getPath().value());
                            Instant now = Instant.now();
                            
                            // Get correlation ID or generate new one if not exists
                            String correlationId = request.getHeaders().getFirst("X-Correlation-Id");
                            if (correlationId == null) {
                                correlationId = UUID.randomUUID().toString();
                            }

                            // Get or extract operator information
                            String jwt = request.getHeaders().getFirst("Authorization");
                            String operatorId = request.getHeaders().getFirst("X-Operator-Id");
                            
                            // Set default operator information if not available
                            if (operatorId == null) {
                                operatorId = "SYSTEM"; // Default system operator
                            }
                            
                            String operatorName = null;
                            if (jwt != null) {
                                operatorName = extractUsernameFromJwt(jwt);
                            }
                            if (operatorName == null) {
                                operatorName = "SYSTEM"; // Default system operator name
                            }
                            
                            // Create detailed error message
                            String errorMessage = String.format("Error processing request to %s: %s", 
                                currentServiceName, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());

                            // Get current request method
                            String requestMethod = request.getMethod() != null ? request.getMethod().name() : "";
                            
                            // Determine log type based on the current state
                            String finalLogType = determineLogType(
                                exchange.getResponse().getStatusCode() != null ? 
                                exchange.getResponse().getStatusCode().value() : 0,
                                request.getPath().value(),
                                System.currentTimeMillis() - start,
                                requestMethod
                            );

                            errorEvent.put("eventId", errorEventId);
                            errorEvent.put("logType", finalLogType);  // Use determined log type
                            errorEvent.put("action", finalLogType.equals("ERROR") ? "LOG_PROCESSING_ERROR" : requestMethod + "_REQUEST");
                            errorEvent.put("serviceName", currentServiceName);
                            errorEvent.put("severity", finalLogType.equals("ERROR") ? "ERROR" : "INFO");
                            errorEvent.put("eventTimestamp", now.toString());
                            errorEvent.put("createdAt", now.toString());
                            errorEvent.put("operatorId", operatorId);
                            errorEvent.put("operatorName", operatorName);
                            errorEvent.put("correlationId", correlationId);
                            errorEvent.put("message", errorMessage);
                            
                            // Add error details to payload with guaranteed non-null values
                            Map<String, Object> errorPayload = new HashMap<>();
                            errorPayload.put("errorMessage", e.getMessage() != null ? e.getMessage() : "Unknown error");
                            errorPayload.put("errorType", e.getClass().getSimpleName());
                            errorPayload.put("originalPath", request.getPath().value());
                            errorPayload.put("serviceName", currentServiceName);
                            errorPayload.put("timestamp", now.toString());
                            errorPayload.put("requestMethod", request.getMethod() != null ? request.getMethod().name() : "UNKNOWN");
                            errorPayload.put("requestHeaders", getHeadersAsMap(request));
                            errorPayload.put("clientIP", request.getRemoteAddress() != null ? 
                                request.getRemoteAddress().getAddress().getHostAddress() : "UNKNOWN");
                            
                            errorEvent.put("payload", errorPayload);
                            
                            // Always include tags
                            List<String> tags = generateTags("ERROR", 500, 0);
                            if (tags == null || tags.isEmpty()) {
                                tags = new ArrayList<>();
                                tags.add("error");
                            }
                            errorEvent.put("tags", tags);
                            
                            // Set expireAt for log retention
                            Instant expireAt = now.plus(30, ChronoUnit.DAYS);
                            errorEvent.put("expireAt", expireAt.toString());

                            String errorJson = objectMapper.writeValueAsString(errorEvent);
                            kafkaTemplate.send(TOPIC, errorJson);
                        } catch (Exception ex) {
                            log.error("Critical: Failed to send error event to Kafka", ex);
                        }
                    }
                });
    }

    private String determineServiceName(String path) {
        if (path.startsWith("/api/users")) return "user-service";
        if (path.startsWith("/api/monitoring")) return "monitoring-service";
        return "api-gateway";
    }

    private String determineLogType(int status, String path, long latency, String method) {
        // Determine service first
        String serviceName = determineServiceName(path);
        
        if (serviceName.equals("api-gateway")) {
            return "ACCESS"; // Nếu là gateway endpoint, luôn là ACCESS
        }

        // Nếu là request đến service khác
        if (!isServiceAvailable(serviceName)) {
            return "ERROR"; // Service không available
        }

        // Kiểm tra response status
        if (status >= 400) {
            return "ERROR";  // Lỗi client hoặc server
        }

        if (status >= 200 && status < 300) {
            return "GATEWAY"; // Request được forward thành công
        }

        return "ACCESS"; // Mặc định là ACCESS cho các trường hợp còn lại
    }

    private String determineAction(String method, String path, int status, long latency) {
        if (status >= 500) return "SERVER_ERROR_" + status;
        if (status >= 400) return "CLIENT_ERROR_" + status;
        if (path.contains("/auth")) return method + "_AUTH";
        if (path.contains("/login")) return method + "_LOGIN";
        if (path.contains("/logout")) return method + "_LOGOUT";
        if (latency > 2000) return "SLOW_RESPONSE";
        return method + "_" + determineBusinessAction(path);
    }

    private String determineBusinessAction(String path) {
        if (path.contains("/users")) {
            if (path.endsWith("/users")) return "USER";
            if (path.contains("/activate")) return "USER_ACTIVATION";
            if (path.contains("/password")) return "PASSWORD_CHANGE";
        }
        return "REQUEST";
    }

    private String determineSeverity(int status, long latency) {
        if (status >= 500) return "ERROR";
        if (status >= 400 || latency > 2000) return "WARN";
        return "INFO";
    }

    private String username;  // Store extracted username for later use

    private String generateEventId(String serviceName) {
        return String.format("EVT_%d_%s_%s",
                System.currentTimeMillis(),
                serviceName.toUpperCase().replace("-", "_"),
                UUID.randomUUID().toString().substring(0, 6));
    }

    private Map<String, String> getHeadersAsMap(ServerHttpRequest request) {
        Map<String, String> headerMap = new HashMap<>();
        HttpHeaders headers = request.getHeaders();
        headers.forEach((key, value) -> {
            if (!key.toLowerCase().contains("auth")) { // Skip sensitive headers
                headerMap.put(key, String.join(", ", value));
            }
        });
        return headerMap;
    }

    private String extractUsernameFromJwt(String jwt) {
        if (jwt == null || !jwt.startsWith("Bearer ")) return null;
        try {
            String[] parts = jwt.split("\\.");
            if (parts.length != 3) return null;
            
            String payload = new String(Base64.getDecoder().decode(parts[1]));
            Map<String, Object> claims = objectMapper.readValue(payload, Map.class);
            username = (String) claims.get("sub");  // Store username for later use
            return username;
        } catch (Exception e) {
            log.warn("Failed to extract username from JWT", e);
            username = null;
            return null;
        }
    }

    private List<String> generateTags(String logType, int status, long latency) {
        List<String> tags = new ArrayList<>();
        tags.add(logType.toLowerCase());
        
        if (status >= 500) tags.add("error");
        if (status >= 400) tags.add("warning");
        if (latency > 2000) tags.add("slow");
        
        return tags;
    }

    @Autowired
    private DiscoveryClient discoveryClient;

    private boolean isServiceAvailable(String serviceName) {
        try {
            List<ServiceInstance> instances = discoveryClient.getInstances(serviceName);
            return !instances.isEmpty();
        } catch (Exception e) {
            log.error("Error checking service availability for {}: {}", serviceName, e.getMessage());
            return false;
        }
    }

    @Override
    public int getOrder() {
        return -1; // highest priority
    }
}