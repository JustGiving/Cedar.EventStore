﻿namespace Cedar.EventStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;

    public sealed class StreamSubscription : SubscriptionBase, IStreamSubscription
    {
        private readonly string _streamId;
        private int _nextVersion;
        private int _lastVersion;

        public StreamSubscription(
            string streamId,
            int startVersion,
            IReadOnlyEventStore readOnlyEventStore,
            IObservable<Unit> eventStoreAppendedNotification,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped,
            string name = null)
            :base(readOnlyEventStore, eventStoreAppendedNotification, streamEventReceived, subscriptionDropped, name)
        {
            _streamId = streamId;
            _nextVersion = startVersion;
            _lastVersion = startVersion - 1;
        }

        public string StreamId => _streamId;

        public int LastVersion => _lastVersion;

        public override async Task Start(CancellationToken cancellationToken)
        {
            if(_nextVersion == StreamVersion.End)
            {
                // Get the last stream version and subscribe from there.
                var eventsPage = await ReadOnlyEventStore.ReadStreamBackwards(
                    _streamId,
                    StreamVersion.End,
                    1,
                    cancellationToken).NotOnCapturedContext();

                //Only new events, i.e. the one after the current last one 
                _nextVersion = eventsPage.LastStreamVersion + 1;
            }
            await base.Start(cancellationToken);
        }

        protected override async Task<bool> DoFetch()
        {
            var streamEventsPage = await ReadOnlyEventStore
                .ReadStreamForwards(
                    _streamId,
                    _nextVersion,
                    PageSize,
                    IsDisposed)
                .NotOnCapturedContext();
            bool isEnd = streamEventsPage.IsEndOfStream;

            foreach(var streamEvent in streamEventsPage.Events)
            {
                if(IsDisposed.IsCancellationRequested)
                {
                    return true;
                }
                _nextVersion = streamEvent.StreamVersion + 1;
                _lastVersion = streamEvent.StreamVersion;
                try
                {
                    await StreamEventReceived(streamEvent).NotOnCapturedContext();
                }
                catch(Exception ex)
                {
                    try
                    {
                        SubscriptionDropped.Invoke(ex.Message, ex);
                    }
                    catch(Exception ex2)
                    {
                        // Need to log this 
                    }
                    finally
                    {
                        Dispose();
                    }
                }
            }
            return isEnd;
        }
    }
}
