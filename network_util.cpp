#include "err.h"
#include "network_util.h"

#include <ctime>
#include <bits/stdc++.h>

using namespace std;


string get_current_time() {
    time_t now = time(nullptr);
    struct tm *tm_info = localtime(&now);
    if(tm_info == NULL) return "unknown";
    char buf[32];
    strftime(buf, sizeof(buf), "%Y.%m.%d %H.%M.%S", tm_info);
    return string(buf);
}

#define HTTP_DEFAULT_PORT "80"
#define HTTPS_DEFAULT_PORT "443"

/*
Attempts to parse 'url', interpreting 
*/
void parse_url_without_brackets(const string &url, string &protocol,
    string &host, string &port, string &path, int verbosity) {

    // Determine protocol.
    size_t end_protocol = url.find("://");
    if(end_protocol == string::npos) {
        fatal(verbosity, "Invalid url structure: protocol.");
    }
    protocol = url.substr(0, end_protocol);
    transform(protocol.begin(), protocol.end(), protocol.begin(), ::tolower);

    // Set default ports.
    if(protocol == "http") port = HTTP_DEFAULT_PORT;
    else if(protocol == "https") port = HTTPS_DEFAULT_PORT;
    else fatal(verbosity, "Invalid url structure: protocol.");
 
    // Determine if a port was specified. Set host and port accordingly.
    size_t begin_port = url.find(":", end_protocol + 3);
    size_t begin_path = url.find("/", end_protocol + 3);
    // We treat no "/" character as a sign of an empty path and set up guard.
    if(begin_path == string::npos) begin_path = url.length();

    bool has_port = begin_port != string::npos && (begin_port < begin_path);
    
    if(has_port) {
        // Port specified - host is between protocol and port separators.
        host = url.substr(end_protocol + 3, begin_port - (end_protocol + 3));
        port = url.substr(begin_port + 1, begin_path - (begin_port + 1));
    }
    else {
        host = url.substr(end_protocol + 3, begin_path - (end_protocol + 3));
    }

    // Set empty path ("/") if not specified.
    if(begin_path == url.length()) path = "/";
    else path = url.substr(begin_path);
}

void parse_url_with_brackets(const string &url, string &protocol,
    string &host, string &port, string &path, int verbosity) {

    // Determine protocol.
    size_t end_protocol = url.find("://");
    if(end_protocol == string::npos) {
        fatal(verbosity, "Invalid url structure: protocol.");
    }
    protocol = url.substr(0, end_protocol);
    transform(protocol.begin(), protocol.end(), protocol.begin(), ::tolower);
    
    // Set default ports.
    if(protocol == "http") port = HTTP_DEFAULT_PORT;
    else if(protocol == "https") port = HTTPS_DEFAULT_PORT;
    else fatal(verbosity, "Invalid url structure: protocol.");

    // Extract IPv6 host from literal placed between the brackets.
    size_t left_bracket = url.find("[", end_protocol);
    size_t right_bracket = url.find("]", left_bracket + 1);
    if(left_bracket == string::npos || right_bracket == string::npos) {
        fatal(verbosity, "Invalid url structure: broken IPv6 host.");
    }
    host = url.substr(left_bracket + 1, right_bracket - (left_bracket + 1));

    // Determine if a port was specified. Set host and port accordingly.
    size_t begin_port = url.find(":", right_bracket + 1);
    size_t begin_path = url.find("/", right_bracket + 1);
    // We treat no "/" character as a sign of an empty path and set up guard.
    if(begin_path == string::npos) begin_path = url.length();

    bool has_port = begin_port != string::npos && (begin_port < begin_path);

    if(has_port) {
        port = url.substr(begin_port + 1, begin_path - (begin_port + 1));
    }

    // Set empty path ("/") if not specified.
    if(begin_path == url.length()) path = "/";
    else path = url.substr(begin_path);
}

string get_hr_ip_port(const addrinfo* ai, int verbosity) {
    char host[NI_MAXHOST];
    char serv[NI_MAXSERV];

    int errcode = getnameinfo(ai->ai_addr, ai->ai_addrlen, host, sizeof(host), 
        serv, sizeof(serv), NI_NUMERICHOST | NI_NUMERICSERV);
    if(errcode != 0) {
        noncritical(verbosity, "getnameinfo failed.");
        return "unknown";
    }

    if(ai->ai_family == AF_INET) { // IPv4
        return string(host) + ":" + string(serv);
    } 
    else if(ai->ai_family == AF_INET6) { // IPv6
        return "[" + string(host) + "]:" + string(serv);
    }
    else return "unknown";
}

int connect_tcp(const string &host, const string &port, bool force_ip4, 
    bool force_ip6, int verbosity) {

    if(verbosity >= COMMUNICATION) {
        cerr << get_current_time() << endl;
        cerr << "resolving name " << host << endl;
    }

    addrinfo hints{};

    // No flags or conflicting flags.
    if((force_ip4 && force_ip6) || (!force_ip4 && !force_ip6)) {
        hints.ai_family = AF_UNSPEC; // IPv4 or IPv6
    }
    else if(force_ip4) {
        hints.ai_family = AF_INET; // IPv4
    }
    else if(force_ip6) {
        hints.ai_family = AF_INET6; // IPv6
    }

    hints.ai_socktype = SOCK_STREAM; // TCP

    addrinfo* res = nullptr;
    int errcode = getaddrinfo(host.c_str(), port.c_str(), &hints, &res);
    if(errcode != 0) {
        syserr(verbosity, "getaddrinfo");
    }

    int sock = -1;
    for(addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
        if(verbosity >= DIAGNOSTIC) {
            cerr << "attempting connection to " << get_hr_ip_port(ai, verbosity)
                << endl;
        }
        sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if(sock < 0) {
            continue;
        }
        if(connect(sock, ai->ai_addr, ai->ai_addrlen) == 0) {
            if(verbosity >= COMMUNICATION) {
                cerr << "connecting to server " << 
                    get_hr_ip_port(ai, verbosity) << endl;
            }
            break;
        }
        if(close(sock) != 0) noncritical(verbosity, "Problems closing socket.");
        sock = -1;
    }

    if(sock == -1) fatal(verbosity, "Failed to connect to server.");
    freeaddrinfo(res);
    return sock;
}

string build_request(const string& path, const string& host,
    const map <string, string>& cookies, bool multiplex) {
    string req;

    req += "GET " + path + " HTTP/1.1\r\n";
    if(host.find(":") != string::npos) { // Host is a IPv6 literal.
        req += "Host: [" + host + "]\r\n";
    }
    else req += "Host: " + host + "\r\n";

    req += "Connection: Keep-Alive\r\n";
    if(multiplex) req += "Icy-MetaData: 1\r\n";
    if(!cookies.empty()) {
        req += "Cookie: ";
        bool first_cookie = true;
        for(auto it : cookies) {
            if(!first_cookie) req += "; "; // We do not want a trailing "; ".
            first_cookie = false;
            req += it.first + "=" + it.second;
        }
        req += "\r\n";
    }
    req += "\r\n";

    return req;
}

#define HTTP_STATUS_MIN 100
#define HTTP_STATUS_MAX 599

int get_status(const string &header) {
    // In both formats the status comes right after specifying the protocol.
    size_t first_blank = header.find(" ");
    if(first_blank == string::npos) return -1;
    int status = atoi(header.substr(first_blank + 1, 3).c_str());
    if(status < HTTP_STATUS_MIN || status > HTTP_STATUS_MAX) return -1;
    return status;
}

bool header_complete(const string &header) {
    size_t n = header.length();
    if(n >= 4 && header.substr(n - 4, 4) == "\r\n\r\n") {
        return true;
    }
    return false;
}

/*
Removes (in place) whitespaces from prefix and suffix of string.
*/
void trim_whitespaces(string& s) {
    s.erase(0, s.find_first_not_of(" \t\r\n"));
    s.erase(s.find_last_not_of(" \t\r\n") + 1);
}

void get_header_fields(const string &header, 
    multimap <string, string> &fields) {
    size_t line_end = header.find("\n");
    size_t separator, line_start;
    while(line_end != string::npos) {
        line_start = line_end;
        separator = header.find(":", line_start + 1);
        line_end = header.find("\n", line_start + 1);
        if(separator == string::npos || line_end == string::npos
            || line_end < separator) {
            // Slightly malformed header, but we will ignore this line and 
            // proceed.
            continue;
        }
        else {
            string key = header.substr(line_start + 1, 
                separator - (line_start + 1));
            string value = header.substr(separator + 1, 
                line_end - (separator + 1));
            trim_whitespaces(key);
            trim_whitespaces(value);
            // Also convert keys to lowercase for more compatibility.
            transform(key.begin(), key.end(), key.begin(), ::tolower);
            fields.emplace(key, value);
        }
    }
    return;
}

#define COOKIE_FIELD_NAME "set-cookie"

void update_cookies(multimap <string, string> &fields, 
    map <string, string> &cookies) {

    for(auto it : fields) {
        if(it.first == COOKIE_FIELD_NAME) {
            // Looking for pattern: {name}={value};
            size_t name_end = it.second.find("="); 
            if(name_end == string::npos) continue;
            size_t value_end = it.second.find(";", name_end);

            string name = it.second.substr(0, name_end - 0);
            // Cookie names are case insensitive.
            transform(name.begin(), name.end(), name.begin(), ::tolower);
            // Absent ; indicates that the rest of the string is the value.
            if(value_end == string::npos) {
                cookies[name] = it.second.substr(name_end + 1);
            }
            else {
                cookies[name] = it.second.substr(name_end + 1, 
                    value_end - (name_end + 1));
            }
        }
    }

    return;
}

/*
Takes a new url given by a redirection and the old url split into its 
fields (protocol, host, port, path).
Returns the new url correctly combined with the old values.
*/
string consider_relative_path(const string &new_url, string &protocol,
    string &host, string &port, string &path) {
        if(new_url.find("://") == string::npos) {
            string base = protocol + "://";
            if(host.find(":") != string::npos) { // IPv6 literal
                base += "[" + host + "]";  
            } 
            else {
                base += host;
            }
            base += ":" + port;

            if(new_url[0] != '/') { // Relative to current directory
                size_t last_slash = path.rfind('/');
                if(last_slash != string::npos) {
                    base += path.substr(0, last_slash + 1);
                } 
                else {
                    base += "/";
                }
            }
            return base + new_url;
        }
        else return new_url;
}