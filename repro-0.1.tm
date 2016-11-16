namespace eval ::repro {
    # This namespace contains the default options for all created REDIS
    # connections.
    namespace eval vars {
        variable -eol "\r\n"
        variable -port 6379
        variable -auth ""
    }
    namespace export {[a-z]*}
    namespace ensemble create
}

# ::repro::connect -- Connect to REDIS host
#
#       Connect to a redis host and return an identifier for the connection.
#       This identifier will be used in the remaining exported commands of the
#       library. Apart from the hostname, this procedure can take the following
#       options:
#       -eol    Marker for End-of-Line in REDIS protocol, you shouldn't really change!
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
proc ::repro::connect { host args } {
    defaults CX vars {*}$args
    
    # Now open the socket to the remote server.
    set sock [socket $host [dict get $CX -port]]
    fconfigure $sock -translation binary

    # Create a variable, in this namespace, using the (unique) name of the
    # socket as the name, copy information from the CX dictionary into it.
    set cx [namespace current]::$sock
    dict for {k v} $CX {
        dict set $cx $k $v
    }
    dict set $cx sock $sock; # Remember the socket, even though it's obvious
    
    # Authenticate at server now that we have a connection
    if { [dict get $CX -auth] ne "" } {
        set ok [sync $sock AUTH [dict get $CX -auth]]
        if { $ok ne "OK" } {
            unset $cx
            return -code error "Authentication failure at server"
        }
    }
    
    return $sock;   # Return the socket, its our identifier!
}


# ::repro::defaults -- Init and option parsing based on namespace.
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
proc ::repro::defaults { cx_ ns args } {
    upvar $cx_ CX

    # Set defaults out of library defaults in vars sub-namespace
    foreach v [uplevel info vars [string trimright $ns :]::-*] {
        dict set CX [lindex [split $v :] end] [set $v]
    }
    
    # Get options from arguments and set their values, only if these are valid
    # options.
    set parsed [list]
    foreach {k v} $args {
        set k -[string trimleft $k -]
        if { [dict exists $CX $k] } {
            dict set CX $k $v
            lappend parsed $k
        } else {
            return -code error "$k is not a valid option"
        }
    }
    
    return $parsed
}


# ::repro::disconnect -- Disconnect from server
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
proc ::repro::disconnect { sock } {
    set cx [namespace current]::$sock
    if { ![info exists $cx] } {
        return -code error "$sock is not a known connection"
    }
    
    unset $cx
    close $sock;   # Let it fail on purpose
}


# ::repro::command -- Send command to server, return its answer
#
#       Sends a REDIS command to a remote server, and wait for its answer.
#       The command will automatically be converted to uppercase, if necessary.
#
# Arguments:
#	sock	Identifier of the REDIS connection, as returned from connect
#	cmd	Command to send
#	args	Arguments to send
#
# Results:
#       The result of the command is returned, maybe as a list if the answer was
#       coded in multi-bulk format.
#
# Side Effects:
#       Might generate errors when writing/reading data to/from socket
proc ::repro::command { sock cmd args } {
    # Check that this is an existing connection
    set cx [namespace current]::$sock
    if { ![info exists $cx] } {
        return -code error "$sock is not a known connection"
    }

    send $sock $cmd {*}$args    
    return [reply $sock]
}


# ::repro::send -- Send command to server
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
proc ::repro::send { sock cmd args } {
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


# ::repro::reply -- Read reply from server
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
proc ::repro::reply { sock } {
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


proc ::repro::MWrite { sock cmd args } {
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

proc ::repro::Write { sock cmd args } {
    puts -nonewline $sock [string toupper $cmd]
    if { [llength $args] } {
        puts -nonewline $sock " "
        puts -nonewline $sock $args
    }
    puts -nonewline $sock ${vars::-eol}
    
    flush $sock
}

proc ::repro::ReadMultiBulk { sock } {
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


proc ::repro::ReadBulk { sock } {
    set len [ReadLine $sock]
    if { $len < 0 } {
        return
    }
    set buf [read $sock $len]
    read $sock [string length ${vars::-eol}]
    return $buf
}

proc ::repro::ReadLine { sock } {
    return [string trim [gets $sock]]
}

