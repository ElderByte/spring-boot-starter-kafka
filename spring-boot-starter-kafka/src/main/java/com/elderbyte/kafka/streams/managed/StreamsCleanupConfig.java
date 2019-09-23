package com.elderbyte.kafka.streams.managed;

public class StreamsCleanupConfig {

	private final boolean onStart;

	private final boolean onStop;

	private final boolean onError;

	public StreamsCleanupConfig() {
		this(false, true, false);
	}

	public StreamsCleanupConfig(boolean onStart, boolean onStop, boolean onError) {
		this.onStart = onStart;
		this.onStop = onStop;
		this.onError = onError;
	}

	public boolean cleanupOnStart() {
		return this.onStart;
	}

	public boolean cleanupOnStop() {
		return this.onStop;
	}

	public boolean cleanupOnError() {
		return this.onError;
	}

}
