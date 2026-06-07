package com.atlassystems.missioncontrol.sse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import java.util.concurrent.CopyOnWriteArrayList;
@Slf4j
@Component
public class SseEmitterRegistry {
    private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();
    public SseEmitter register() {
        SseEmitter em = new SseEmitter(Long.MAX_VALUE);
        emitters.add(em);
        em.onCompletion(()->emitters.remove(em));
        em.onTimeout(()->emitters.remove(em));
        log.info("[SSE] Client connected. Total: {}", emitters.size());
        return em;
    }
    public void broadcast(SseEvent event) {
        emitters.removeIf(em -> { try { em.send(SseEmitter.event().name(event.type()).data(event.payload())); return false; }
            catch(Exception e){ return true; } });
    }
}
