namespace Cedar.EventStore.Streams
{
    using System;
    using EnsureThat;

    public sealed class NewStreamEvent
    {
        public readonly byte[] JsonData;
        public readonly Guid EventId;
        public readonly string Type;
        public readonly byte[] JsonMetadata;

        public NewStreamEvent(Guid eventId, string type, byte[] jsonData, byte[] metadata = null)
        {
            Ensure.That(eventId, "eventId").IsNotEmpty();
            Ensure.That(type, "type").IsNotNullOrEmpty();
            Ensure.That(jsonData, "data").IsNotNull();

            EventId = eventId;
            Type = type;
            JsonData = jsonData;
            JsonMetadata = metadata;
        }
    }
}