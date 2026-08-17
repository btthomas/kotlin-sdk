package dev.openfeature.kotlin.sdk

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Coordinates lifecycle state for overlapping context changes. The mutex protects bookkeeping;
 * provider callbacks run outside it and are not queued.
 */
internal class ContextReconciler(
    private val getStatus: () -> OpenFeatureStatus,
    private val emitStatus: suspend (OpenFeatureStatus) -> Unit
) {
    internal data class Reconciliation(
        val oldContext: EvaluationContext?,
        val provider: FeatureProvider,
        val providerGeneration: Long
    )

    private data class TerminalStatus(
        val status: OpenFeatureStatus,
        val providerStatusGeneration: Long
    )

    private data class State(
        var providerGeneration: Long? = null,
        var activeCount: Int = 0,
        var initialStatus: OpenFeatureStatus? = null,
        var terminalStatus: TerminalStatus? = null,
        var providerStatusGeneration: Long = 0
    )

    private val mutex = Mutex()
    private val state = State()

    suspend fun begin(
        capture: suspend () -> Reconciliation?,
        onRegistered: (Reconciliation) -> Unit
    ): Unit = mutex.withLock {
        val reconciliation = capture() ?: return@withLock
        if (state.providerGeneration != reconciliation.providerGeneration) {
            state.providerGeneration = reconciliation.providerGeneration
            state.activeCount = 0
        }
        if (state.activeCount == 0) {
            state.initialStatus = getStatus()
            state.terminalStatus = null
        }
        state.activeCount++
        // Publish the token before emitting so cancellation can still complete the registration.
        onRegistered(reconciliation)
        if (state.activeCount == 1) {
            emitStatus(OpenFeatureStatus.Reconciling)
        }
    }

    suspend fun complete(
        reconciliation: Reconciliation,
        terminalStatus: OpenFeatureStatus?,
        isCurrentProvider: suspend () -> Boolean
    ) = mutex.withLock {
        if (state.providerGeneration != reconciliation.providerGeneration) return@withLock

        if (terminalStatus != null) {
            state.terminalStatus = TerminalStatus(terminalStatus, state.providerStatusGeneration)
        }
        state.activeCount--
        if (state.activeCount != 0) return@withLock

        val retainedTerminalStatus = state.terminalStatus
        val statusToEmit = retainedTerminalStatus?.status ?: state.initialStatus
        val shouldEmitStatus = if (retainedTerminalStatus != null) {
            retainedTerminalStatus.providerStatusGeneration == state.providerStatusGeneration
        } else {
            getStatus() is OpenFeatureStatus.Reconciling
        }
        state.initialStatus = null
        state.terminalStatus = null

        if (statusToEmit != null && shouldEmitStatus && isCurrentProvider()) {
            emitStatus(statusToEmit)
        }
    }

    suspend fun emitProviderStatus(status: OpenFeatureStatus) = mutex.withLock {
        state.providerStatusGeneration++
        emitStatus(status)
    }
}