package db.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import db.instructions.Instruction;

/**
 * @author vlad333rrty
 */
public record Transaction(IsolationLevel isolationLevel, List<Instruction> instructions, String id) {
    public enum IsolationLevel {
        READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
    }

    public static class Builder {
        private final List<Instruction> instructions = new ArrayList<>();
        private IsolationLevel isolationLevel = IsolationLevel.SERIALIZABLE;

        public Builder addInstruction(Instruction instruction) {
            instructions.add(instruction);
            return this;
        }

        public Builder setIsolationLevel(IsolationLevel isolationLevel) {
            this.isolationLevel = isolationLevel;
            return this;
        }

        public Transaction build() {
            return new Transaction(isolationLevel, instructions, generateFQDN());
        }

        private String generateFQDN() {
            return UUID.randomUUID().toString();
        }
    }
}
