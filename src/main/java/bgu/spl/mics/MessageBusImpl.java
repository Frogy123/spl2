package bgu.spl.mics;

import java.util.concurrent.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * MessageBus implementation:
 * - Keeps the same public interface and behavior as the given interface.
 * - Internals are changed for different style/structure:
 *   * Subscriber registries use ConcurrentLinkedQueue and round-robin is done
 *     via queue rotation (poll -> offer).
 *   * Mailboxes remain LinkedBlockingQueue to preserve blocking semantics.
 *   * Futures remain mapped per concrete Event instance.
 */
public class MessageBusImpl implements MessageBus {

	/** One blocking mailbox per registered micro-service */
	private final ConcurrentMap<MicroService, LinkedBlockingQueue<Message>> inboxes =
			new ConcurrentHashMap<>();

	/** Subscribers per event type (round-robin via rotation) */
	private final ConcurrentMap<Class<? extends Event<?>>, ConcurrentLinkedQueue<MicroService>> eventSubs =
			new ConcurrentHashMap<>();

	/** Subscribers per broadcast type */
	private final ConcurrentMap<Class<? extends Broadcast>, ConcurrentLinkedQueue<MicroService>> broadcastSubs =
			new ConcurrentHashMap<>();

	/** Futures attached to concrete event instances */
	private final ConcurrentMap<Event<?>, Future<?>> futures = new ConcurrentHashMap<>();

	// SINGLETON:
	private static class MessageBusHolder {
		private static final MessageBus messageBus = new MessageBusImpl();
	}

	private MessageBusImpl() {
		// nothing to initialize beyond field initializers
	}

	public static MessageBus getInstance() {
		return MessageBusHolder.messageBus;
	}

	// ---------- subscription management ----------

	@Override
	public <T> void subscribeEvent(Class<? extends Event<T>> type, MicroService m) {
		ensureRegistered(m);
		@SuppressWarnings("unchecked")
		Class<? extends Event<?>> key = (Class<? extends Event<?>>) type;
		eventSubs.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>()).add(m);
	}

	@Override
	public void subscribeBroadcast(Class<? extends Broadcast> type, MicroService m) {
		ensureRegistered(m);
		broadcastSubs.computeIfAbsent(type, k -> new ConcurrentLinkedQueue<>()).add(m);
	}

	// ---------- message dispatch ----------

	@Override
	public <T> Future<T> sendEvent(Event<T> e) {
		@SuppressWarnings("unchecked")
		Class<? extends Event<?>> t = (Class<? extends Event<?>>) e.getClass();
		ConcurrentLinkedQueue<MicroService> subs = eventSubs.get(t);
		if (subs == null || subs.isEmpty()) {
			return null; // nobody listens
		}

		// Round-robin without counters: rotate the queue until we find a registered target.
		int attempts = subs.size();
		LinkedBlockingQueue<Message> mailbox = null;
		MicroService target = null;

		while (attempts-- > 0) {
			MicroService cand = subs.poll(); // head
			if (cand == null) break;
			subs.offer(cand);                // rotate
			LinkedBlockingQueue<Message> q = inboxes.get(cand);
			if (q != null) {                 // still registered
				mailbox = q;
				target = cand;
				break;
			}
		}

		if (mailbox == null) {
			return null; // all subscribers gone/unregistered
		}

		Future<T> f = new Future<>();
		futures.put(e, f);
		mailbox.offer(e);
		return f;
	}

	@Override
	public void sendBroadcast(Broadcast b) {
		ConcurrentLinkedQueue<MicroService> subs = broadcastSubs.get(b.getClass());
		if (subs == null || subs.isEmpty()) {
			return;
		}
		// Snapshot subscribers to avoid concurrent structural changes during iteration.
		MicroService[] snapshot = subs.toArray(new MicroService[0]);
		for (MicroService m : snapshot) {
			LinkedBlockingQueue<Message> q = inboxes.get(m);
			if (q != null) {
				q.offer(b);
			}
		}
	}

	@Override
	public <T> void complete(Event<T> e, T result) {
		@SuppressWarnings("unchecked")
		Future<T> f = (Future<T>) futures.remove(e);
		if (f != null) {
			f.resolve(result);
		}
	}

	// ---------- registration lifecycle ----------

	@Override
	public void register(MicroService m) {
		inboxes.computeIfAbsent(m, k -> new LinkedBlockingQueue<>());
	}

	@Override
	public void unregister(MicroService m) {
		// Drop mailbox first (pending messages to it will be discarded).
		inboxes.remove(m);

		// Remove from all subscription registries.
		for (ConcurrentLinkedQueue<MicroService> q : eventSubs.values()) {
			q.remove(m);
		}
		for (ConcurrentLinkedQueue<MicroService> q : broadcastSubs.values()) {
			q.remove(m);
		}
	}

	@Override
	public Message awaitMessage(MicroService m) throws InterruptedException {
		LinkedBlockingQueue<Message> q = inboxes.get(m);
		if (q == null) {
			throw new IllegalStateException("MicroService " + m.getName() + " is not registered");
		}
		return q.take(); // blocking until a message arrives
	}

	// ---------- testing helpers (same signature as provided interface) ----------

	@Override
	public Boolean isRegistered(MicroService m) {
		return inboxes.containsKey(m);
	}

	@Override
	public Boolean isSubscribedToBroadcast(Class<? extends Broadcast> type, MicroService m) {
		ConcurrentLinkedQueue<MicroService> q = broadcastSubs.get(type);
		return q != null && q.contains(m);
	}

	@Override
	public <T> Boolean isSubscribedToEvent(Class<? extends Event<T>> type, MicroService m) {
		@SuppressWarnings("unchecked")
		Class<? extends Event<?>> key = (Class<? extends Event<?>>) type;
		ConcurrentLinkedQueue<MicroService> q = eventSubs.get(key);
		return q != null && q.contains(m);
	}

	// ---------- helpers ----------

	private void ensureRegistered(MicroService m) {
		if (!isRegistered(m)) {
			throw new IllegalStateException("MicroService " + m.getName() + " must be registered first");
		}
	}
}
