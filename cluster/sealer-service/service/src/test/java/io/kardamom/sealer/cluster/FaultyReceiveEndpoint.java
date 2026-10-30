package io.kardamom.sealer.cluster;

import io.aeron.driver.DataPacketDispatcher;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ReceiveChannelEndpointSupplier;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationTransport;
import io.aeron.driver.media.UdpChannel;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * A receive endpoint of a member's media driver that applies the
 * {@link LinkFaults} to the data, setup and RTT frames that arrive from
 * peers. It covers the frames of a multi-destination subscription too: the
 * driver hands those to the endpoint as well.
 */
public final class FaultyReceiveEndpoint extends ReceiveChannelEndpoint implements LinkFaults.Tickable {

    /** The supplier that {@code aeron.ReceiveChannelEndpoint.supplier} names. */
    public static final class Supplier implements ReceiveChannelEndpointSupplier {
        @Override
        public ReceiveChannelEndpoint newInstance(
                final UdpChannel udpChannel,
                final DataPacketDispatcher dispatcher,
                final AtomicCounter statusIndicator,
                final MediaDriver.Context context) {
            return LinkFaults.get()
                .<ReceiveChannelEndpoint>map(f -> new FaultyReceiveEndpoint(udpChannel, dispatcher, statusIndicator,
                    context, f))
                .orElseGet(() -> new ReceiveChannelEndpoint(udpChannel, dispatcher, statusIndicator, context));
        }
    }

    private final HeldFrames held;
    private final List<ReceiveDestinationTransport> destinations = new CopyOnWriteArrayList<>();

    private FaultyReceiveEndpoint(
            final UdpChannel udpChannel,
            final DataPacketDispatcher dispatcher,
            final AtomicCounter statusIndicator,
            final MediaDriver.Context context,
            final LinkFaults faults) {
        super(udpChannel, dispatcher, statusIndicator, context);
        this.held = new HeldFrames(faults, this::dispatch);
        faults.register(this);
    }

    @Override
    public int addDestination(final ReceiveDestinationTransport transport) {
        destinations.add(transport);
        return super.addDestination(transport);
    }

    @Override
    public List<DatagramChannel> channels() {
        return java.util.stream.Stream.concat(
                java.util.stream.Stream.of(receiveDatagramChannel()),
                destinations.stream().map(ReceiveDestinationTransport::receiveDatagramChannel))
            .toList();
    }

    @Override
    public ByteBuffer tick() {
        return LinkFaults.tickFrame(DataHeaderFlyweight.HEADER_LENGTH, HeaderFlyweight.HDR_TYPE_PAD);
    }

    @Override
    public int onDataPacket(
            final DataHeaderFlyweight header,
            final UnsafeBuffer buffer,
            final int length,
            final InetSocketAddress srcAddress,
            final int transportIndex) {
        return held.intercept(buffer, length, srcAddress, transportIndex)
            ? 0 : super.onDataPacket(header, buffer, length, srcAddress, transportIndex);
    }

    @Override
    public void onSetupMessage(
            final SetupFlyweight header,
            final UnsafeBuffer buffer,
            final int length,
            final InetSocketAddress srcAddress,
            final int transportIndex) {
        if (!held.intercept(buffer, length, srcAddress, transportIndex)) {
            super.onSetupMessage(header, buffer, length, srcAddress, transportIndex);
        }
    }

    @Override
    public void onRttMeasurement(
            final RttMeasurementFlyweight header,
            final UnsafeBuffer buffer,
            final int length,
            final InetSocketAddress srcAddress,
            final int transportIndex) {
        if (!held.intercept(buffer, length, srcAddress, transportIndex)) {
            super.onRttMeasurement(header, buffer, length, srcAddress, transportIndex);
        }
    }

    private void dispatch(
            final UnsafeBuffer frame, final int length, final InetSocketAddress source, final int transportIndex) {
        switch (frame.getShort(HeaderFlyweight.TYPE_FIELD_OFFSET, java.nio.ByteOrder.LITTLE_ENDIAN)) {
            case HeaderFlyweight.HDR_TYPE_SETUP ->
                super.onSetupMessage(new SetupFlyweight(frame), frame, length, source, transportIndex);
            case HeaderFlyweight.HDR_TYPE_RTTM ->
                super.onRttMeasurement(new RttMeasurementFlyweight(frame), frame, length, source, transportIndex);
            default -> super.onDataPacket(new DataHeaderFlyweight(frame), frame, length, source, transportIndex);
        }
    }
}
