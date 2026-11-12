package io.kardamom.sealer.cluster;

import io.aeron.driver.DriverConductorProxy;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.SendChannelEndpointSupplier;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.ResponseSetupFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * A send endpoint of a member's media driver that applies the
 * {@link LinkFaults} to the status, NAK, RTT and response setup frames that
 * arrive from peers: the control frames that a peer's receiver sends back
 * to a publication of this member.
 */
public final class FaultySendEndpoint extends SendChannelEndpoint implements LinkFaults.Tickable {

    /** The supplier that {@code aeron.SendChannelEndpoint.supplier} names. */
    public static final class Supplier implements SendChannelEndpointSupplier {
        @Override
        public SendChannelEndpoint newInstance(
                final UdpChannel udpChannel, final AtomicCounter statusIndicator, final MediaDriver.Context context) {
            return LinkFaults.get()
                .<SendChannelEndpoint>map(f -> new FaultySendEndpoint(udpChannel, statusIndicator, context, f))
                .orElseGet(() -> new SendChannelEndpoint(udpChannel, statusIndicator, context));
        }
    }

    private final HeldFrames held;
    /** The conductor proxy of the driver, from the first status frame; a held status frame needs it. */
    private DriverConductorProxy conductor;

    private FaultySendEndpoint(
            final UdpChannel udpChannel,
            final AtomicCounter statusIndicator,
            final MediaDriver.Context context,
            final LinkFaults faults) {
        super(udpChannel, statusIndicator, context);
        this.held = new HeldFrames(faults, this::dispatch);
        faults.register(this);
    }

    @Override
    public List<DatagramChannel> channels() {
        return java.util.stream.Stream.of(receiveDatagramChannel()).toList();
    }

    @Override
    public ByteBuffer tick() {
        return LinkFaults.tickFrame(RttMeasurementFlyweight.HEADER_LENGTH, HeaderFlyweight.HDR_TYPE_RTTM);
    }

    @Override
    public void onStatusMessage(
            final StatusMessageFlyweight msg,
            final UnsafeBuffer buffer,
            final int length,
            final InetSocketAddress srcAddress,
            final DriverConductorProxy conductorProxy) {
        conductor = conductorProxy;
        if (!held.intercept(buffer, length, srcAddress, 0)) {
            super.onStatusMessage(msg, buffer, length, srcAddress, conductorProxy);
        }
    }

    @Override
    public void onNakMessage(
            final NakFlyweight msg, final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress) {
        if (!held.intercept(buffer, length, srcAddress, 0)) {
            super.onNakMessage(msg, buffer, length, srcAddress);
        }
    }

    @Override
    public void onRttMeasurement(
            final RttMeasurementFlyweight msg,
            final UnsafeBuffer buffer,
            final int length,
            final InetSocketAddress srcAddress) {
        if (!held.intercept(buffer, length, srcAddress, 0)) {
            super.onRttMeasurement(msg, buffer, length, srcAddress);
        }
    }

    @Override
    public void onResponseSetup(
            final ResponseSetupFlyweight msg,
            final UnsafeBuffer buffer,
            final int length,
            final InetSocketAddress srcAddress,
            final DriverConductorProxy conductorProxy) {
        conductor = conductorProxy;
        if (!held.intercept(buffer, length, srcAddress, 0)) {
            super.onResponseSetup(msg, buffer, length, srcAddress, conductorProxy);
        }
    }

    private void dispatch(
            final UnsafeBuffer frame, final int length, final InetSocketAddress source, final int transportIndex) {
        switch (frame.getShort(HeaderFlyweight.TYPE_FIELD_OFFSET, java.nio.ByteOrder.LITTLE_ENDIAN)) {
            case HeaderFlyweight.HDR_TYPE_NAK -> super.onNakMessage(new NakFlyweight(frame), frame, length, source);
            case HeaderFlyweight.HDR_TYPE_RTTM ->
                super.onRttMeasurement(new RttMeasurementFlyweight(frame), frame, length, source);
            case HeaderFlyweight.HDR_TYPE_RSP_SETUP ->
                super.onResponseSetup(new ResponseSetupFlyweight(frame), frame, length, source, conductor);
            default -> super.onStatusMessage(new StatusMessageFlyweight(frame), frame, length, source, conductor);
        }
    }
}
