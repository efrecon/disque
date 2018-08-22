##################
## Module Name     --  resp.tm
## Original Author --  Emmanuel Frecon - emmanuel@sics.se
## Description:
##
##     This module implements the base functionality to talk to remote REDIS
##     servers through the RESP protocol (REdis Serialisation Protocol).
##
##################

package require Tcl 8.6

namespace eval ::resp {
    # This namespace contains the default options for all created REDIS
    # connections.
    namespace eval vars {
        variable -eol "\r\n"
        variable -port 6379
        variable -auth ""
        variable version       [lindex [split [file rootname [file tail [info script]]] -] end]        
    }
    namespace export {[a-z]*}
    namespace ensemble create
}

# ::resp::connect -- Connect to REDIS host
#
#       Connect to a redis host and return an identifier for the connection.
#       This identifier will be used in the remaining exported commands of the
#       library. Apart from the hostname, this procedure can take the following
#       options:
#       -port   Port to connect to, defaults the default REDIS port.
#       -auth   Password to authenticate with at connection.
#
# Arguments:
#	host	Name of host to connect to
#	args	Dash-led options and their values, see above.
#
# Results:
#       Return the identifier for the connection, used throughout this library.
#       Return errors on connection problems or authentication failure.
#
# Side Effects:
#       None.
proc ::resp::connect { host args } {
    # Get options, use the global vars namespace as the source for good
    # defaults.
    getopt args -port port ${vars::-port}
    getopt args -auth paswd ${vars::-auth}
    
    # Now open the socket to the remote server.
    set sock [socket $host $port]
    fconfigure $sock -translation binary

    # Create a variable, in this namespace, using the (unique) name of the
    # socket as the name, copy information from the CX dictionary into it.
    set cx [namespace current]::$sock
    upvar \#0 $cx CX
    dict set CX -port $port
    dict set CX -auth $paswd
    dict set CX -host $host
    dict set CX sock  $sock; # Remember the socket, even though it's obvious
    
    # Authenticate at server now that we have a connection
    if { $paswd ne "" } {
        set ok [sync $sock AUTH $paswd]
        if { $ok ne "OK" } {
            unset $cx
            return -code error "Authentication failure at server"
        }
    }
    
    return $sock;   # Return the socket, its our identifier!
}


# ::resp::disconnect -- Disconnect from server
#
#       Disconnect from a REDIS server and empty all references to the server.
#
# Arguments:
#	sock	Identifier of the REDIS connection, as returned from connect
#
# Results:
#       None.
#
# Side Effects:
#       Might generate errors on connection closing.
proc ::resp::disconnect { sock } {
    set cx [namespace current]::$sock
    if { ![info exists $cx] } {
        return -code error "$sock is not a known connection"
    }
    
    unset $cx
    close $sock;   # Let it fail on purpose
}


# ::resp::command -- Send command to server, return its answer
#
#       Sends a REDIS command to a remote server, and possibly wait for its answer.
#       The command will automatically be converted to uppercase, if necessary.
#       The arguments can contain a number of dash-led options and their value,
#       separated from the remaining of the arguments by a double-dash. The only
#       option at present is -reply, which contains a callback that will be
#       triggered with the content of the reply once it has been made available.
#
# Arguments:
#	sock	Identifier of the REDIS connection, as returned from connect
#	cmd	Command to send
#	args	Arguments to send, possibly led by dash-led options
#
# Results:
#       The result of the command is returned, maybe as a list if the answer was
#       coded in multi-bulk format. When called in asynchronous mode, this
#       procedure will callback the trigger with the content of the answer when
#       it has been made available.
#
# Side Effects:
#       Might generate errors when writing/reading data to/from socket
proc ::resp::command { sock cmd args } {
    # Check that this is an existing connection
    set cx [namespace current]::$sock
    if { ![info exists $cx] } {
        return -code error "$sock is not a known connection"
    }

    isolate args opts
    send $sock $cmd {*}$args
    if { [getopt opts -reply cb] } {
        fileevent $sock readable [list ReadAndCallback sock cb]
    } else {
        return [reply $sock]
    }
}


# ::resp::send -- Send command to server
#
#       Sends a REDIS command to a remote server, not waiting for its answer.
#       The command will automatically be converted to uppercase, if necessary.
#
# Arguments:
#	sock	Identifier of the REDIS connection, as returned from connect
#	cmd	Command to send
#	args	Arguments to send
#
# Results:
#       None.
#
# Side Effects:
#       Might generate errors when writing data to socket
proc ::resp::send { sock cmd args } {
    # Check that this is an existing connection
    set cx [namespace current]::$sock
    if { ![info exists $cx] } {
        return -code error "$sock is not a known connection"
    }
    
    if { [llength $args] == 0 } {
        Write $sock $cmd {*}$args
    } else {
        MWrite $sock $cmd {*}$args
    }
}


# ::resp::reply -- Read reply from server
#
#       Read the reply from a command that has been sent to the server using the
#       procedure send. This implementation is aware of all the packetisation
#       types known to REDIS.
#
# Arguments:
#	sock	Identifier of the REDIS connection, as returned from connect
#
# Results:
#       Return the result of the command, which is represented as a list of
#       replies when the answer is in multi bulk.
#
# Side Effects:
#       Might generate errors when reading data from socket
proc ::resp::reply { sock } {
    # Check that this is an existing connection
    set cx [namespace current]::$sock
    if { ![info exists $cx] } {
        return -code error "$sock is not a known connection"
    }

    set type [read $sock 1]
    switch -exact -- $type {
        ":" -
        "+" {
            return [ReadLine $sock]
        }
        "-" {
            set err [ReadLine $sock]
            return -code error $err
        }
        "$" {
            return [ReadBulk $sock]
        }
        "*" {
            return [ReadMultiBulk $sock]
        }
        default {
            return -code error "$type is an unknown REDIS header type"
        }
    }
}


# ::resp::getopt -- Get options
#
#       From http://wiki.tcl.tk/17342
#
# Arguments:
#	_argv	"pointer" to incoming arguments
#	name	Name of option to extract
#	_var	Pointer to variable to set
#	default	Default value
#
# Results:
#       1 if the option was found, 0 otherwise
#
# Side Effects:
#       None.
proc ::resp::getopt {_argv name {_var ""} {default ""}} {
    upvar 1 $_argv argv $_var var
    set pos [lsearch -regexp $argv ^$name]
    if {$pos>=0} {
        set to $pos
        if {$_var ne ""} {
            set var [lindex $argv [incr to]]
        }
        set argv [lreplace $argv $pos $to]
        return 1
    } else {
        if {[llength [info level 0]] == 5} {set var $default}
        return 0
    }
}

 
# ::resp::defaults -- Init and option parsing based on namespace.
#
#       This procedure takes the dashled variables of a given (sub)namespace to
#       initialise a dictionary. These variables are considered as being the
#       canonical set of options for a command or object and contain good
#       defaults, and the procedure will capture these from the arguments.
#
# Arguments:
#	cx_	"Pointer" to dictionary to initialise and parse options in.
#	ns	Namespace (FQ or relative to caller) where to get options from
#	args	List of dashled options and arguments, must match content of namespace
#
# Results:
#       Return the list of options that were taken from the arguments, an error
#       when an option that does not exist in the namespace as a variable was
#       found in the arguments.
#
# Side Effects:
#       None.
proc ::resp::defaults { cx_ ns args } {
    upvar $cx_ CX

    set parsed [list]
    foreach v [uplevel info vars [string trimright $ns :]::-*] {
        set opt [lindex [split $v :] end]
        if { [getopt args $opt value [set $v]] } {
            lappend parsed $opt
        }
        dict set CX $opt $value
    }
    
    return $parsed
}


# ::resp::isolate -- Isolate options from arguments
#
#       Isolate dash-led options from the rest of the arguments. This procedure
#       prefers the double-dash as a marker between the options and the
#       arguments, but it is also able to traverse until the end of the options
#       and the beginning of the arguments. Traversal requires that no value of
#       an option starts with a dash to work properly.
#
# Arguments:
#	args_	Pointer to list of arguments (will be modified!)
#	opts_	Pointer to list of options
#
# Results:
#       None.
#
# Side Effects:
#       Modifies the args and opts lists that are passed as parameters to
#       reflect the arguments and the options.
proc ::resp::isolate { args_ opts_ } {
    upvar $args_ args $opts_ opts
    set idx [lsearch $args "--"]
    if { $idx >= 0 } {
        set opts [lrange $args 0 [expr {$idx-1}]]
        set args [lrange $args [expr {$idx+1}] end]
    } else {
        set opts [list]
        for {set i 0} {$i <[llength $args] } { incr i 2} {
            set opt [lindex $args $i]
            set val [lindex $args [expr {$i+1}]]
            if { [string index $opt 0] eq "-" } {
                if { [string index $val 0] eq "-" } {
                    incr i -1; # Consider next not next-next!
                    lappend opts $opt
                } else {
                    lappend opts $opt $val
                }
            } else {
                break
            }
        }
        set args [lrange $args $i end]
    }    
}



# ::resp::ReadAndCallback -- Callback with response
#
#       This procedure is bound to the socket when it is ready to have data for
#       reading. It will read a complete response from the server and deliver a
#       callback with the response.
#
# Arguments:
#	sock	Socket to read reply from
#	cb	Command to callback
#
# Results:
#       None.
#
# Side Effects:
#       None.
proc ::resp::ReadAndCallback { sock cb } {
    fileevent $sock readable {}
    set response [reply $sock]
    if { [catch {{*}$cb $reply} err] } {
        return -code error "Could not callback with reply: $err"
    }
}


# ::resp::MWrite -- Multi-bulk write
#
#       Write a command and its arguments using the multi-bulk protocol. This is
#       especially usefull when sending complex bodies of jobs for DISQUE.
#
# Arguments:
#	sock	Socket to REDIS node.
#	cmd	Command to send
#	args	Arguments to command, each will lead to a "bulk"
#
# Results:
#       None.
#
# Side Effects:
#       None.
proc ::resp::MWrite { sock cmd args } {
    set len [llength $args];
    incr len;  # Count the command as well
    puts -nonewline $sock "*$len${vars::-eol}"
    
    set len [string length $cmd]
    puts -nonewline $sock "\$$len${vars::-eol}$cmd${vars::-eol}"
    foreach arg $args {
        set len [string length $arg]
        puts -nonewline $sock "\$$len${vars::-eol}$arg${vars::-eol}"
    }
    flush $sock
}


# ::resp::Write -- Write in regular mode
#
#       Send a command and its arguments to the REDIS server in the regular
#       "one-line" mode.
#
# Arguments:
#	sock	Socket to REDIS node
#	cmd	Command to send
#	args	Arguments to command.
#
# Results:
#       None.
#
# Side Effects:
#       None.
proc ::resp::Write { sock cmd args } {
    puts -nonewline $sock [string toupper $cmd]
    if { [llength $args] } {
        puts -nonewline $sock " "
        puts -nonewline $sock $args
    }
    puts -nonewline $sock ${vars::-eol}
    
    flush $sock
}


# ::resp::ReadMultiBulk -- Read several bulks
#
#       Read a multi-bulk formatted block of data from the REDIS socket.
#
# Arguments:
#	sock	Socket to REDIS node
#
# Results:
#       Return a list of each bulk that was read.
#
# Side Effects:
#       None.
proc ::resp::ReadMultiBulk { sock } {
    set len [ReadLine $sock]
    if { $len < 0 } {
        return
    }
    
    set reply [list]
    for { set i 0 } { $i < $len } { incr i } {
        lappend reply [reply $sock]
    }
    return $reply
}


# ::resp::ReadBulk -- Read one bulk
#
#       Read one bulk from the REDIS socket.
#
# Arguments:
#	sock	Socket to REDIS node
#
# Results:
#       Return the bulk that was read
#
# Side Effects:
#       None.
proc ::resp::ReadBulk { sock } {
    set len [ReadLine $sock]
    if { $len < 0 } {
        return
    }
    set buf [read $sock $len]
    read $sock [string length ${vars::-eol}]
    return $buf
}


# ::resp::ReadLine -- Read one line
#
#       Read a single line from the REDIS socket.
#
# Arguments:
#	sock	Socket to REDIS node.
#
# Results:
#       Read line that was read, leading and trailing spaces are trimmed away.
#
# Side Effects:
#       None.
proc ::resp::ReadLine { sock } {
    return [string trim [gets $sock]]
}

package provide resp $::resp::vars::version