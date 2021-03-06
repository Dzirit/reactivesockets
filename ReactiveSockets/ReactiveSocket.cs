﻿namespace ReactiveSockets
{
    using System;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Net.Sockets;
    using System.Reactive.Concurrency;
    using System.Reactive.Linq;
    using System.Reactive.Subjects;
    using System.Threading;
    using System.Threading.Tasks;
    using System.IO;
    using System.Reactive;
    using System.Reactive.Threading.Tasks;
    

    /// <summary>
    /// Implements the reactive socket base class, which is used 
     /// as well as a base class for the <see cref="ReactiveClient"/>.
    /// </summary>
    public class ReactiveSocket : IReactiveSocket, IDisposable
    {
       

        private bool disposed;
        private TcpClient _tcpClient;
        // This allows us to write to the underlying socket in a 
        // single-threaded fashion.
        private object syncLock = new object();
        private IDisposable readSubscription;

        // This allows protocols to be easily built by consuming 
        // bytes from the stream using Rx expressions.
        // We use a maximum capacity of 1MB, so we don't fill the 
        // collection endlessly and risk an OutOfMemoryException
        private BlockingCollection<byte> received;

        // The receiver created from the above blocking collection.
        private IObservable<byte> receiver;
        // Used to complete the receiver observable
        private Subject<Unit> receiverTermination = new Subject<Unit>(); 

        // Subject used to pub/sub sent bytes.
        private ISubject<byte> sender = new Subject<byte>();

        // The default receive buffer size of TcpClient according to
        // http://msdn.microsoft.com/en-us/library/system.net.sockets.tcpclient.receivebuffersize.aspx
        // is 8192 bytes
        private int receiveBufferSize = 8192;

        /// <summary>
        /// The maximum number of bytes that is stored to process for the <see cref="Receiver"/> observable.
        /// </summary>
        public const int MaximumBufferSize = 1024 * 1024;

        /// <summary>
        /// Initializes the socket with a previously accepted TCP 
        /// </summary>
        internal ReactiveSocket(TcpClient tcpClient)
            : this(MaximumBufferSize)
        {
            
            Connect(tcpClient);
        }

        /// <summary>
        /// Protected constructor used by <see cref="ReactiveClient"/> 
        /// client.
        /// </summary>
        protected internal ReactiveSocket(int maximumBufferSize)
        {
            if(maximumBufferSize < 0)
                throw new ArgumentOutOfRangeException("maximumBufferSize", "Maximum buffer size must be greater than 0");

            received = new BlockingCollection<byte>(maximumBufferSize);
            receiver = received.GetConsumingEnumerable().ToObservable(TaskPoolScheduler.Default)
                .TakeUntil(receiverTermination);
        }

        /// <summary>
        /// Raised when the socket is connected.
        /// </summary>
        public event EventHandler Connected = (sender, args) => { };

        /// <summary>
        /// Raised when the socket is disconnected.
        /// </summary>
        public event EventHandler Disconnected = (sender, args) => { };

        /// <summary>
        /// Raised when the socket is disposed.
        /// </summary>
        public event EventHandler Disposed = (sender, args) => { };

        /// <summary>
        /// Gets whether the socket is connected.
        /// </summary>
        public bool IsConnected { get { return _tcpClient != null && _tcpClient.Connected; } }

        /// <summary>
        /// Observable bytes that are being received by this endpoint. Note that 
        /// subscribing to the receiver blocks until a byte is received, so 
        /// subscribers will typically use the extension method <c>SubscribeOn</c> 
        /// to specify the scheduler to use for subscription.
        /// </summary>
        /// <remarks>
        /// This blocking characteristic also propagates to higher level channels built 
        /// on top of this socket, but it's not necessary to use SubscribeOn 
        /// at more than one level.
        /// </remarks>
        public IObservable<byte> Receiver { get { return receiver; } }

        /// <summary>
        /// Observable bytes that are being sent through this endpoint 
        /// by using the <see cref="SendAsync(byte[])"/> or 
        /// <see cref="SendAsync(byte[], CancellationToken)"/>  methods.
        /// </summary>
        public IObservable<byte> Sender { get { return sender; } }

        /// <summary>
        /// Gets or sets the <see cref="TcpClient.ReceiveBufferSize"/> of the 
        /// underlying <see cref="TcpClient"/>.
        /// The default value is 8192 bytes.
        /// </summary>
        public int ReceiveBufferSize
        {
            get { return this.receiveBufferSize; }
            set
            {
                this.receiveBufferSize = value;

                if(this._tcpClient != null)
                {
                    this._tcpClient.ReceiveBufferSize = value;
                }
            }
        }

        /// <summary>
        /// Gets the TcpClient stream to use. 
        /// </summary>
        /// <remarks>Virtual so it can be overridden to implement SSL</remarks>
        protected virtual Stream GetStream()
        {
            return _tcpClient.GetStream();
        }

        /// <summary>
        /// Connects the reactive socket using the given TCP client.
        /// </summary>
        protected internal void Connect(TcpClient tcp)
        {
            if (tcp == null)
                throw new ArgumentNullException("tcp");

            if (disposed)
            {
               
                throw new ObjectDisposedException(this.ToString());
            }

            if (!tcp.Connected)
            {
               
                throw new InvalidOperationException("Client must be connected");
            }

            // We're connecting an already connected client.
            if (_tcpClient == tcp && tcp.Connected)
            {
                
                return;
            }

            // We're switching to a new client?
            if (_tcpClient != null && _tcpClient != tcp)
            {
                
                Disconnect();
            }

            this._tcpClient = tcp;
            tcp.ReceiveBufferSize = receiveBufferSize;

            // Cancel possibly outgoing async work (i.e. reads).
            if (readSubscription != null)
            {
                readSubscription.Dispose();
            }

            // Subscribe to the new client with the new token.
            BeginRead();

            Connected(this, EventArgs.Empty);

            
        }

        /// <summary>
        /// Disconnects the reactive socket. Throws if not currently connected.
        /// </summary>
        public virtual void Disconnect()
        {
            //if (!IsConnected)
            //    throw new InvalidOperationException(Strings.TcpClientSocket.DisconnectingNotConnected);

            Disconnect(false);
        }

        /// <summary>
        /// Disconnects the socket, specifying if this is being called 
        /// from Dispose.
        /// </summary>
        protected void Disconnect(bool disposing)
        {
            if (disposed && !disposing)
                throw new ObjectDisposedException(this.ToString());

            if (readSubscription != null)
            {
                readSubscription.Dispose();
            }

            readSubscription = null;

            if (IsConnected)
            {
                _tcpClient.Close();
                
            }

            _tcpClient = null;

            Disconnected(this, EventArgs.Empty);
        }

        /// <summary>
        /// Disconnects the socket and releases all resources.
        /// </summary>
        public void Dispose()
        {
            if (disposed)
                return;

            disposed = true;

            Disconnect(true);

            sender.OnCompleted();
            receiverTermination.OnNext(Unit.Default);

            

            Disposed(this, EventArgs.Empty);
        }

        private void BeginRead()
        {
            Stream stream = this.GetStream();
            this.readSubscription = Observable.Defer(() => 
                {
                    var buffer = new byte[this.ReceiveBufferSize];
                    return stream.ReadAsync(buffer, 0, buffer.Length).ToObservable()
                        .Select(x => buffer.Take(x).ToArray());
                })
                .Repeat()
                .TakeWhile(x => x.Any())
                .SelectMany(x => x)
                .Subscribe(x => this.received.Add(x), ex =>
                {
                    
                    Disconnect(false);
                }, () => Disconnect(false));
        }

        /// <summary>
        /// Sends data asynchronously through this endpoint.
        /// </summary>
        public Task SendAsync(byte[] bytes)
        {
            return SendAsync(bytes, CancellationToken.None);
        }

        /// <summary>
        /// Sends data asynchronously through this endpoint, with support 
        /// for cancellation.
        /// </summary>
        public Task SendAsync(byte[] bytes, CancellationToken cancellation)
        {
            if (disposed)
            {
                
                throw new ObjectDisposedException(this.ToString());
            }

            if (!IsConnected)
            {
                
                throw new InvalidOperationException("Not connected");
            }
            
            var stream = this.GetStream();
            return Observable.Start(() => 
            {
                Monitor.Enter(syncLock);
                try  { stream.Write(bytes, 0, bytes.Length); }
                finally { Monitor.Exit(syncLock); }
            })
            .SelectMany(_ => bytes)
            .Do(x => sender.OnNext(x), ex => Disconnect())
            .ToTask(cancellation);
        }

        #region SocketOptions

        /// <summary>See <see cref="T:System.Net.Sockets.Socket.GetSocketOption(SocketOptionLevel, SocketOptionName)" />.</summary>
        public object GetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName)
        {
            return _tcpClient.Client.GetSocketOption(optionLevel, optionName);
        }

        /// <summary>See <see cref="T:System.Net.Sockets.Socket.GetSocketOption(SocketOptionLevel, SocketOptionName, byte[])" />.</summary>
        public void GetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, byte[] optionValue)
        {
            _tcpClient.Client.GetSocketOption(optionLevel, optionName, optionValue);
        }

        /// <summary>See <see cref="T:System.Net.Sockets.Socket.GetSocketOption(SocketOptionLevel, SocketOptionName, int)" />.</summary>
        public byte[] GetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, int optionLength)
        {
            return _tcpClient.Client.GetSocketOption(optionLevel, optionName, optionLength);
        }

        /// <summary>See <see cref="T:System.Net.Sockets.Socket.SetSocketOption(SocketOptionLevel, SocketOptionName, bool)" />.</summary>
        public void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, bool optionValue)
        {
            _tcpClient.Client.SetSocketOption(optionLevel, optionName, optionValue);
        }

        /// <summary>See <see cref="T:System.Net.Sockets.Socket.SetSocketOption(SocketOptionLevel, SocketOptionName, byte[])" />.</summary>
        public void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, byte[] optionValue)
        {
            _tcpClient.Client.SetSocketOption(optionLevel, optionName, optionValue);
        }

        /// <summary>See <see cref="T:System.Net.Sockets.Socket.SetSocketOption(SocketOptionLevel, SocketOptionName, int)" />.</summary>
        public void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, int optionValue)
        {
            _tcpClient.Client.SetSocketOption(optionLevel, optionName, optionValue);
        }

        /// <summary>See <see cref="T:System.Net.Sockets.Socket.SetSocketOption(SocketOptionLevel, SocketOptionName, object)" />.</summary>
        public void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, object optionValue)
        {
            _tcpClient.Client.SetSocketOption(optionLevel, optionName, optionValue);
        }

        #endregion
    }
}
