#include "err.h"
#include "network_util.h"

#include <bits/stdc++.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <sys/poll.h>
using namespace std;

#define TIMEOUT_DEFAULT 5000
#define TIMEOUT_MIN 100
#define TIMEOUT_MAX 100000
#define VERBOSITY_DEFAULT 2
#define VERBOSITY_MIN 0
#define VERBOSITY_MAX 4

static string url = "", original_url = "";
static bool multiplex = false;
static int timeout = TIMEOUT_DEFAULT;
static bool force_ip4 = false;
static bool force_ip6 = false;
static int verbosity = VERBOSITY_DEFAULT;

/*
Parses the arguments and assigns the variables above.
We begin adhering to verbosity guidelines only after fully parsing the args.
This means that all critical errors will have their error messages printed.
*/
static void parse_args(int argc, char *argv[]) {
    int opt;
    while((opt = getopt(argc, argv, "u:mt:46v:q")) != -1) {
        switch (opt) {
            case 'u':
                url = string(optarg);
                break;
            case 'm':
                multiplex = true;
                break;
            case 't':
                try {
                    size_t pos;
                    timeout = stoi(string(optarg), &pos);
                    if(pos != strlen(optarg)) {
                        fatal(CRITICAL, "Invalid timeout.");
                    }
                    if((timeout < TIMEOUT_MIN) || (timeout > TIMEOUT_MAX)) {
                        fatal(CRITICAL, "Invalid timeout.");
                    }
                }
                catch (...) {
                    fatal(CRITICAL, "Invalid timeout.");
                }
                break;
            case '4':
                force_ip4 = true;
                break;
            case '6':
                force_ip6 = true;
                break;
            case 'v':
                try {
                    size_t pos;
                    verbosity = stoi(string(optarg), &pos);
                    if(pos != strlen(optarg)) {
                        fatal(CRITICAL, "Invalid verbosity.");
                    }
                    if((verbosity < VERBOSITY_MIN) ||
                        (verbosity > VERBOSITY_MAX)) {
                        fatal(CRITICAL, "Invalid verbosity.");
                    }
                }
                catch (...) {
                    fatal(CRITICAL, "Invalid verbosity.");
                }
                break;
            case 'q':
                verbosity = 0;
                break;
            case '?':
                fatal(CRITICAL, "Unrecognized parameter.");
                break;
            default:
                break;
        }
    }
    if(url.empty()) {
        fatal(CRITICAL, "No url specified.");
    }
}

/*
Section below intends to provide a transparent access to a TCP socket
regardless if HTTP or HTTPS was used.
*/

static SSL_CTX* ctx = nullptr;
static int sock;
static SSL* ssl = nullptr;

static string protocol, host, port, path;

/*
Initializes SSL library.
*/
static void init_ssl() {
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();
    ctx = SSL_CTX_new(TLS_client_method());
    if(!ctx) fatal(verbosity, "SSL_CTX_new failed");
}

/*
Creates an SSL overlay on socket 'sock', used for HTTPS communication.
*/
static void start_ssl() {
    if(protocol == "https") {
        ssl = SSL_new(ctx);
        SSL_set_fd(ssl, sock);
        SSL_set_tlsext_host_name(ssl, host.c_str());
        if(SSL_connect(ssl) != 1) fatal(verbosity, "SSL_connect failed");
    }
}

/*
Attempts to send len bytes through 'sock'. 
Returns the number of bytes sent or the respective error code if sending failed.
*/
static ssize_t net_send(const void* buf, size_t len) {
    if(ssl) return SSL_write(ssl, buf, len);
    else return send(sock, buf, len, 0);
}

/*
Attempts to receive len bytes from 'sock'. 
Returns the number of bytes sent or the respective error code if receive failed.
*/
static ssize_t net_recv(void* buf, size_t len) {
    if(ssl) return SSL_read(ssl, buf, len);
    else return recv(sock, buf, len, 0);
}

/*
Shuts down the SSL overlay on socket 'sock'.
*/
static void end_ssl() {
    if(ssl) {
        SSL_shutdown(ssl);
        SSL_free(ssl);
        ssl = nullptr;
    }
}

/*
Attempts to parse 'url' as both an IPv6 literal (which requires brackets) and
other formats.
Fills variables 'protocol', 'host', 'port', 'path'.
*/
static void parse_url() {
    if(url.find("[") == string::npos) {
        parse_url_without_brackets(url, protocol, host, port, path, verbosity);
    }
    else {
        parse_url_with_brackets(url, protocol, host, port, path, verbosity);
    }
    if(original_url.empty()) original_url = url;
}

/*
Attempts to read the entire HTTP header from the TCP socket byte-by-byte. 
Does so in a blocking manner, which is permitted by the task specification.
Returns the header.
*/
static string read_header() {
    string header;
    bool done = false;
    char c;
    do {
        ssize_t received = net_recv(&c, 1);
        if(received <= 0) {
            fatal(verbosity, "Failed to read entire header.");
        }
        header += c;
        done = header_complete(header);
    }
    while(!done);
    
    return header;
}

#define STD_BUFF_LEN 128
static const string TERMINATION_PHRASE = "quit\n";

static char std_buffer[STD_BUFF_LEN];
static string recent_chars;

/*
Reads from stdin and looks for the termination phrase "quit\n".
Returns true if it was found, false otherwise.
*/
bool parse_std() {
    ssize_t received = read(STDIN_FILENO, std_buffer, sizeof(std_buffer));

    if(received < 0) {
        fatal(verbosity, "Issues with stdin.");
    }
    else if(received == 0) {
        noncritical(verbosity, "stdin has closed.");
        return false;
    }

    recent_chars.append(std_buffer, received);
    if(recent_chars.find(TERMINATION_PHRASE) != string::npos) {
        if(verbosity >= DIAGNOSTIC) {
            cerr << "Quit phrase found." << endl;
        }
        return true;
    }
    if(recent_chars.length() > TERMINATION_PHRASE.length()) {
        recent_chars.erase(0, 
            recent_chars.length() - (TERMINATION_PHRASE.length() - 1));
    }
    return false;
}

#define TCP_BUFF_LEN 65536
static char tcp_buffer[TCP_BUFF_LEN];

/*
Attempts to read from the TCP socket, passing audio stream to stdin and metadata
to stderr (if multiplex is on).
The write to stdin is performed in a blocking fashion - according to the task,
upon receiving a quit signal from user the program is expected to allow the 
remaining audio bytes to be flushed to stdout.
It is completely transparent to the user whether we first receive quit and
flush the remaining bytes afterwards, or keep the audio stream flushed at all
times and immediately exit upon receiving quit.

Takes icy_metaint, the number of audio bytes in each ICY chunk.
Takes and modifies L and position, being the size of the most recent metadata
block, and the byte index we are considering in the current ICY chunk, 
respectively.

Returns true if the server gracefully ended connection, false otherwise.
Calls fatal on abrupt end of connection and other critical errors.
*/
bool parse_tcp(size_t icy_metaint, size_t &L, size_t &position) {
    ssize_t received = net_recv(tcp_buffer, sizeof(tcp_buffer));

    if(received < 0) {
        fatal(verbosity, "Failed to read stream from server.");
    }
    else if(received == 0) {
        return true;
    }
    
    if(icy_metaint == 0) { // No multiplex.
        size_t to_write = received;
        ssize_t written = write(STDOUT_FILENO, tcp_buffer, to_write);
        if(written < (ssize_t) to_write) {
            fatal(verbosity, "Incomplete write on stdout.");
        }
        return false;
    }

    size_t buff_ptr = 0;
    while(received > 0) {
        // We still have some audio bytes to push to stdout.
        if(position < icy_metaint) {
            size_t to_write = min((size_t) received, icy_metaint - position);
            ssize_t written = write(STDOUT_FILENO, &tcp_buffer[buff_ptr], 
                to_write);
            if(written < (ssize_t) to_write) {
                fatal(verbosity, "Incomplete write on stdout.");
            }
            position += to_write;
            received -= to_write;
            buff_ptr += to_write;
        }
        // Find out the size of metadata (L).
        else if(position == icy_metaint) {
            L = ((int) ((unsigned char) tcp_buffer[buff_ptr])) * 16;
            position ++;
            received --;
            buff_ptr ++;
        }
        // We already know the value of L, so proceed with parsing.
        else {
            size_t icy_chunk_size = (icy_metaint + 1 + L);

            // to_print is the number of bytes we are considering till end of
            // meta block.
            size_t to_print = min((size_t) received, 
                (icy_chunk_size) - position);

            // We need to strip the null bytes from the end of metadata block.
            // They are not part of the message.
            size_t no_nullbytes = 0;
            for(ssize_t i = to_print - 1; i >= 0; i --) {
                if(tcp_buffer[buff_ptr + i] == 0) no_nullbytes ++;
                else break;
            }

            // No reason to check for incomplete write - if stderr is broken,
            // then cannot inform the user about this noncritical problem.
            IGNORE_RESULT(write(STDERR_FILENO, &tcp_buffer[buff_ptr], 
                to_print - no_nullbytes)); // Skips the null bytes.
            
            position += to_print;
            if(position == icy_chunk_size) { // Finished meta block.
                position = 0;
                if(L != 0) {
                    char c = '\n';
                    IGNORE_RESULT(write(STDERR_FILENO, &c, 1));
                }
            }

            received -= to_print;
            buff_ptr += to_print;
        }
    }
    return false;
}

enum StreamEndCode {
    USER_ENDED,
    SERVER_DROPPED,
    CONNECTION_TIMEOUT,
};

/*
Continuously polls stdin and TCP socket:
- looks for quit phrase on stdin
- forwards audio from the TCP socket to stdout
- forwards metadata from the TCP socket to stderr (if icy_metaint > 0)

Returns an adequate code if:
- the termination phrase appeared in stdin (USER_ENDED)
- the server ended connection gracefully (SERVER_DROPPED)
- 'timeout' milliseconds passed without data from server (CONNECTION_TIMEOUT)
*/
int receive_stream(size_t icy_metaint) {
    const int std = 0, tcp = 1;
    pollfd fds[2];
    // fds[0] is listening on stdin.
    // fds[1] is listening on our TCP socket.
    fds[std].fd = 0;
    fds[tcp].fd = sock;
    for(int i = 0; i < 2; i ++) {
        fds[i].events = POLLIN | POLLERR | POLLHUP;
        fds[i].revents = 0;
    }

    int remaining_time = timeout;
    size_t L = 0, position = 0;
    while(true) {
        auto start = chrono::steady_clock::now();
        int ret = poll(fds, 2, remaining_time);

        long long elapsed = chrono::duration_cast<chrono::milliseconds>(
            chrono::steady_clock::now() - start).count();
        
        // Introduces slight race condition (with poll timeout).
        // It is possible for poll to wake up with new TCP data, but upon
        // measuring with chrono, the time will have elapsed. In that case,
        // the function ends and returns a timeout code.
        // This situation is tolerable, as this is transparent to the user.
        if(remaining_time - elapsed <= 0) return CONNECTION_TIMEOUT;
        remaining_time = remaining_time - elapsed;

        if(ret < 0) {
            if(errno == EINTR) continue; // Guard against spurious wakeups.
            syserr(verbosity, "poll");
        }
        if(ret == 0) {
            // 'Timeout' milliseconds passed without data from server.
            // Start reconnecting.
            return CONNECTION_TIMEOUT;
        }

        if(fds[std].revents & (POLLIN | POLLERR | POLLHUP)) {
            if(parse_std()) {
                if(verbosity >= DIAGNOSTIC) {
                    cerr << "Ending connection. Flushing remaining bytes." 
                        << endl;
                }
                return USER_ENDED;
            }
            // stdin is closed, no longer listen for it in poll.
            if(fds[std].revents & POLLHUP) fds[std].fd = -1;
        }

        if(fds[tcp].revents & (POLLIN | POLLERR | POLLHUP)) {
            if(parse_tcp(icy_metaint, L, position)) {
                return SERVER_DROPPED;
            }
            // Refresh timeout on succesful receive.
            remaining_time = timeout; 
        }
        fds[std].revents = 0;
        fds[tcp].revents = 0;
        
    }
}

int main(int argc, char *argv[]) {
    parse_args(argc, argv);
    init_ssl();
    multimap <string, string> fields;
    map <string, string> cookies;
    /*
    Begins a redirection/timeout loop.
    If the program receives a redirection response, it keeps the cookies and
    follows to the new location with a new URL.
    If the program receives a status 200 OK response, it starts forwarding the
    audio and metadata from server to user.
    
    Upon receiving a timeout, clears cookies and resets reverts URL to the one
    that was passed in arguments.

    If either the user or the server decide to end the connection, exits with 0.
    */
    while(true) {
        // Establish connection.
        parse_url();
        sock = connect_tcp(host, port, force_ip4, force_ip6, verbosity);
        start_ssl();
        // Create and send request.
        string request = build_request(path, host, port, cookies, multiplex);
        size_t sent = 0;
        while(sent < request.length()) {
            ssize_t n = net_send(request.c_str() + sent,
                request.length() - sent);
            if(n <= 0) fatal(verbosity, "Failed to send request.");
            sent += n;
        }
        if(verbosity >= COMMUNICATION) {
            cerr << request << endl;
        }
        // Receive header.
        string header = read_header();
        if(verbosity >= COMMUNICATION) {
            cerr << header << endl;
        }
        int status = get_status(header);
        if(status == -1) {
            fatal(verbosity, "Unexpected server response.");
        }
        fields.clear();
        get_header_fields(header, fields);
        update_cookies(fields, cookies);
        
        // Handle possible redirection.
        if(status >= MIN_REDIRECTION_STATUS 
            && status <= MAX_REDIRECTION_STATUS) {

            auto it = fields.find("location");
            if(it == fields.end()) {
                fatal(verbosity, "Failed to follow redirect chain.");
            }

            // We found a new address to reconnect to.
            // End the current TCP session (but keep cookies) and start over.
            url = it->second;
            end_ssl();
            if(close(sock) != 0) {
                noncritical(verbosity, "Close had issues.");
            }
            continue;
        }
        // Begin the audio stream.
        else if(status == STATUS_OK) {
            int code;
            if(!multiplex) code = receive_stream(0);
            else {
                auto it = fields.find("icy-metaint");
                if(it == fields.end()) {
                    noncritical(verbosity, "Server does not provide metadata.");
                    code = receive_stream(0);
                }
                else {
                    size_t icy_metaint = atoi(it->second.c_str());
                    code = receive_stream(icy_metaint);
                }
            }
            end_ssl();
            if(close(sock) != 0) {
                noncritical(verbosity, "Close had issues.");
            }
            switch(code) {
                case USER_ENDED:
                    return 0;
                    break;
                case SERVER_DROPPED:
                    return 0;
                    break;
                case CONNECTION_TIMEOUT:
                    if(verbosity >= COMMUNICATION) {
                        cerr << "data receiving timeout" << endl;
                    }
                    cookies.clear();
                    url = original_url;
                    break;
                default:
                    return 1;
            }
        }
        // Invalid status code.
        else {
            fatal(verbosity, "Failed to follow redirect chain.");
        }
    }
}