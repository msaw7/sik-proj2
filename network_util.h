#ifndef SIKRADIO_HTTP_UTIL_H
#define SIKRADIO_HTTP_UTIL_H

#include "err.h"
#include <string>
#include <netdb.h>
#include <map>

#define MIN_REDIRECTION_STATUS 300
#define MAX_REDIRECTION_STATUS 399
#define STATUS_OK 200

using namespace std;

/*
Every function listed below prints error messages to stderr according to passed
verbosity. 
*/

/*
Returns a string with the current time formatted according to the specification.
Returns "unknown" if it failed to acquire current time.
*/
string get_current_time();

void parse_url_without_brackets(const string &url, string &protocol,
    string &host, string &port, string &path, int verbosity);

void parse_url_with_brackets(const string &url, string &protocol,
    string &host, string &port, string &path, int verbosity);

/*
Extracts IP and port from a addrinfo struct.
Returns a string with both presented in human-readable form.
Returns "unknown" if it failed to acquire either.
*/
string get_hr_ip_port(const addrinfo* ai, int verbosity);

/*
Attempts to establish a TCP connection to a server with specified host and port.
Uses getaddrinfo (with specified IP version if force_ipx is set to true) to 
resolve and connects to first valid address.
Returns the file descriptor of a socket it connected on.
Calls fatal if it failed to connect to any address.
*/
int connect_tcp(const string &host, const string &port, bool force_ip4, 
    bool force_ip6, int verbosity);

/*
Creates and returns a string with a HTTP GET request to be sent to server.
Includes a cookie header field with corresponding cookies, if 'cookies' map
is not empty.
Includes "Icy-MetaData: 1\r\n" header field if multiplex is set to true.
*/
string build_request(const string& path, const string& host, const string& port,
    const map <string, string>& cookies, bool multiplex);

/*
Returns the status from a HTTP Response header.
If if failed to extract a valid status, returns -1 instead.
*/
int get_status(const string &header);

/*
Returns true if string 'header' has the HTTP header termination 
phrase ("\r\n\r\n") as its suffix, false otherwise.
*/
bool header_complete(const string &header);

/*
Extracts all header fields from a header and stores them in a multimap.
*/
void get_header_fields(const string &header, multimap <string, string> &fields);

/*
Searches 'fields' for cookie-setting header fields.
Properly extracts them and stores them in 'cookies', overwriting existing ones.
*/
void update_cookies(multimap <string, string> &fields, 
    map <string, string> &cookies);

#endif
