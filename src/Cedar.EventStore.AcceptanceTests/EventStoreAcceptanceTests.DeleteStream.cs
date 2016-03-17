﻿namespace Cedar.EventStore
{
    using System;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task When_delete_existing_stream_with_no_expected_version_then_should_be_deleted()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";
                    var events = new[]
                    {
                        new NewStreamEvent(Guid.NewGuid(), "type", "\"data\"", "\"headers\""),
                        new NewStreamEvent(Guid.NewGuid(), "type", "\"data\"", "\"headers\"")
                    };

                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, events);
                    await eventStore.DeleteStream(streamId);

                    var streamEventsPage =
                        await eventStore.ReadStreamForwards(streamId, StreamVersion.Start, 10);

                    streamEventsPage.Status.ShouldBe(PageReadStatus.StreamDeleted);
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_that_does_not_exist()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "notexist";
                    Func<Task> act = () => eventStore.DeleteStream(streamId);

                    act.ShouldNotThrow();
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_with_a_matching_expected_version_then_should_be_deleted()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";
                    var events = new[]
                    {
                        new NewStreamEvent(Guid.NewGuid(), "type", "\"data\"", "\"headers\""),
                        new NewStreamEvent(Guid.NewGuid(), "type", "\"data\"", "\"headers\"")
                    };

                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, events);
                    await eventStore.DeleteStream(streamId, 1);

                    var streamEventsPage =
                        await eventStore.ReadStreamForwards(streamId, StreamVersion.Start, 10);

                    streamEventsPage.Status.ShouldBe(PageReadStatus.StreamDeleted);
                }
            }
        }

        [Fact(Skip = "To be re-implemented")]
        public async Task When_delete_a_stream_and_append_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";

                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1));
                    await eventStore.DeleteStream(streamId);

                    var exception = await Record.ExceptionAsync(() => 
                        eventStore.AppendToStream(streamId, ExpectedVersion.Any, CreateNewStreamEvents(1)));

                    exception.ShouldBeOfType<StreamDeletedException>();
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_that_does_not_exist_then_should_not_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "notexist";

                    var exception = await Record.ExceptionAsync(() => eventStore.DeleteStream(streamId));

                    exception.ShouldBeNull();
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_that_does_not_exist_with_expected_version_number_then_should_not_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "notexist";
                    const int expectedVersion = 1;

                    var exception = await Record.ExceptionAsync(() =>
                        eventStore.DeleteStream(streamId, expectedVersion));

                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                        Messages.DeleteStreamFailedWrongExpectedVersion(streamId, expectedVersion));
                }
            }
        }

        [Fact]
        public async Task When_delete_stream_with_a_non_matching_expected_version_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    const string streamId = "stream";
                    await eventStore.AppendToStream(streamId, ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    var exception = await Record.ExceptionAsync(() =>
                        eventStore.DeleteStream(streamId, 100));
                    
                    exception.ShouldBeOfType<WrongExpectedVersionException>(
                            Messages.DeleteStreamFailedWrongExpectedVersion(streamId, 100));
                }
            }
        }
    }
}