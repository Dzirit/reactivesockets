using System;
using System.IO;

namespace ReactiveSockets
{
    using System.Net.Sockets;
    using System.Threading.Tasks;
    

    /// <summary>
    /// Implements the <see cref="IReactiveClient"/> over TCP.
    /// </summary>
    public class ReactiveClient : ReactiveSocket, IReactiveClient
    {
        
        private string hostname;
        private int port;
        private readonly Func<Stream, Stream> streamTransform;
        private Stream _stream;
        private readonly object getStreamLock = new object();

        /// <summary>
        /// Initializes the reactive client.
        /// </summary>
        /// <param name="hostname">The host name or IP address of the TCP server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        /// <param name="maximumBufferSize">An optional value for the maximum number of bytes 
        /// that is stored before TCP flow control kicks in.
        /// The default value is <see cref="ReactiveSocket.MaximumBufferSize"/>
        /// </param>
        public ReactiveClient(string hostname, int port, int maximumBufferSize = MaximumBufferSize) 
            : this(hostname, port, _stream => _stream, maximumBufferSize) { }

        /// <summary>
        /// Initializes the reactive client using a custom stream transform.
        /// This transform allows using SslStream to provide a secure communication channel to a server
        /// that requires SSL.
        /// </summary>
        /// <param name="hostname">The host name or IP address of the TCP server to connect to.</param>
        /// <param name="port">The port to connect to.</param>
        /// <param name="streamTransform">The callback function to use to obtain the communication <see cref="Stream"/>.
        /// The callback is passed the original Stream from the underlying <see cref="TcpClient"/>.</param>
        /// <param name="maximumBufferSize">An optional value for the maximum number of bytes 
        /// that is stored before TCP flow control kicks in.
        /// The default value is <see cref="ReactiveSocket.MaximumBufferSize"/>
        /// </param>
        /// <example>
        /// Using with SSL:
        /// <code>
        /// var client  = new ReactiveClient(host, port, stream => {
        ///   var ssl = new SslStream(
        ///     stream, 
        ///     userCertificateValidationCallback: (sender, certificate, chain, errors) => true  // ignore SSL cert validation
        ///   );
        ///   ssl.AuthenticateAsClient(host);
        ///   return ssl;
        /// } 
        /// </code>
        /// </example>
        public ReactiveClient(string hostname, int port, Func<Stream, Stream> streamTransform, int maximumBufferSize = MaximumBufferSize)
            :base(maximumBufferSize)
        {
            this.hostname = hostname;
            this.port = port;
            this.streamTransform = streamTransform;
            
        }

        /// <summary>
        /// Attemps to connect to the TCP server.
        /// </summary>
        public Task ConnectAsync()
        {
            var tcpClient = new TcpClient();
            return tcpClient.ConnectAsync(hostname, port)
                .ContinueWith(t => Connect(tcpClient), TaskContinuationOptions.OnlyOnRanToCompletion);
        }

        /// <summary>
        /// Disconnects the underlying TCP socket.
        /// </summary>
        public override void Disconnect()
        {
            _stream.Close();
            _stream.Dispose();
            _stream = null;
            base.Disconnect();
        }

        /// <summary>
        /// Invoke the streamTransform callback to provide a stream to the underlying read/write methods.
        /// </summary>
        /// <returns></returns>
        protected override Stream GetStream()
        {
            lock (getStreamLock)
            {
                return _stream ??
                       (_stream = (streamTransform == null ? base.GetStream() : streamTransform(base.GetStream())));
            }
        }
    }
}
