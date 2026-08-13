export type ParticipantLifecycleTransition = 'participant.joined' | 'participant.left' | null;

export class MeetingParticipantLifecycle {
  private readonly participants = new Set<string>();
  private readonly pendingPresence = new Map<string, boolean>();
  private started = false;

  begin(currentParticipantIds: Iterable<string | number>): string[] {
    if (this.started) throw new Error('Meeting participant lifecycle already started');

    for (const participantId of currentParticipantIds) this.participants.add(String(participantId));
    for (const [participantId, isPresent] of this.pendingPresence) {
      if (isPresent) this.participants.add(participantId);
      else this.participants.delete(participantId);
    }
    this.pendingPresence.clear();
    this.started = true;
    return [...this.participants];
  }

  observe(participantId: string, isPresent: boolean): ParticipantLifecycleTransition {
    if (!this.started) {
      this.pendingPresence.set(participantId, isPresent);
      return null;
    }
    if (isPresent) {
      if (this.participants.has(participantId)) return null;
      this.participants.add(participantId);
      return 'participant.joined';
    }
    if (!this.participants.delete(participantId)) return null;
    return 'participant.left';
  }
}
