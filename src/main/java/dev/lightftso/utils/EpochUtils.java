package dev.lightftso.utils;

public class EpochUtils {
    public long FLARE_FIRST_EPOCH_TIMESTAMP = 1657740870L * 1000L;
    public long SONGBIRD_FIRST_EPOCH_TIMESTAMP = 1631824801L * 1000L;

    private long DEFAULT_SUBMIT_DURATION = 180 * 1000;
    private long DEFAULT_REVEAL_DURATION = 90 * 1000;

    public final long submitDuration;
    public final long revealDuration;
    public long firstEpochTimestamp;

    public EpochUtils(String network, long submitDuration, long revealDuration) {
        this.setFirstEpochTimestamp(network);
        this.submitDuration = submitDuration;
        this.revealDuration = revealDuration;
    }

    public EpochUtils(String network) {
        this.setFirstEpochTimestamp(network);
        this.submitDuration = DEFAULT_SUBMIT_DURATION;
        this.revealDuration = DEFAULT_REVEAL_DURATION;
    }

    private void setFirstEpochTimestamp(String network) {
        if (network.toUpperCase().equalsIgnoreCase("FLARE")) {
            this.firstEpochTimestamp = this.FLARE_FIRST_EPOCH_TIMESTAMP;
        } else if (network.toUpperCase().equalsIgnoreCase("SONGBIRD")) {
            this.firstEpochTimestamp = this.SONGBIRD_FIRST_EPOCH_TIMESTAMP;
        } else {
            throw new RuntimeException("Error: invalid network selected. Must be one of \"flare\" or \"songbird\"");
        }

    }

    public long getRunningEpoch() {
        return (long) Math.floor((System.currentTimeMillis() - this.firstEpochTimestamp) / this.submitDuration);
    }

    public long getTotalEpochs() {
        return (long) Math.floor((System.currentTimeMillis() - this.firstEpochTimestamp) / this.submitDuration) - 1;
    }

    public long getEpochStartTimestamp(long epochId) {
        return this.firstEpochTimestamp + epochId * this.submitDuration;
    }

    public long getEpochEndTimestamp(long epochId) {
        return this.getEpochStartTimestamp(epochId) + this.submitDuration;
    }

    public long getEpochFromTimestamp(long timestamp) {
        return (long) Math.floor((timestamp - this.firstEpochTimestamp) / this.submitDuration);
    }

    public long getDeltaFromTimestamp(long timestamp) {
        var epoch = this.getEpochFromTimestamp(timestamp);
        var submit_end = this.getEpochEndTimestamp(epoch);
        return submit_end - timestamp;
    }

    public long getLastFinishedEpochStartTimestamp() {
        var last_epoch_submit_start = this.getEpochStartTimestamp(this.getLastFinishedEpoch());
        return last_epoch_submit_start;
    }

    public long getLastFinishedEpochEndTimestamp() {
        var last_epoch_submit_start = this.getEpochStartTimestamp(this.getLastFinishedEpoch());
        return last_epoch_submit_start + this.submitDuration;
    }

    public long getLastFinishedEpoch() {
        return this.getTotalEpochs();
    }
}
