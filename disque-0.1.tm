##################
## Module Name     --  disque.tm
## Original Author --  Emmanuel Frecon - emmanuel@sics.se
## Description:
##
##     This module provides a high-level interface to the DISQUE distributed
##     message broker. DISQUE is available at https://github.com/antirez/disque
##
##################

package require Tcl 8.6

package require repro;   # repro implements the REDIS protocol

namespace eval ::disque {
    namespace eval vars {
        variable id 0;        # Identifier generator
        variable -version 1;  # Protocol version
        variable -id -;       # Separator for ids.
        variable -idlen 40;   # Exact length of job ids.
    }
    # This holds the default options for all new instances.
    namespace eval disque {
        variable -port 7711;    # Default port when not specified
        variable -nodes [list localhost]
        variable -liveness [list]
        variable -auth "";      # Default auth if not part of node specification.
        variable -grace 5000;   # Grace period when shutting down node
        variable -polldown 100; # Poll (in ms) for detecting jobs end on shutdown
    }
    # This holds the defaults options for new jobs.
    namespace eval addjob {
        variable -timeout 1000
        variable -replicate -1
        variable -delay -1;    # Will mean default of Disque
        variable -retry -1;    # Will mean default
        variable -ttl -1;      # Will mean default of Disque
        variable -maxlen -1;   # Will mean no maxlen
        variable -async off;
    }
    # This holds the defaults options for getting jobs from queues
    namespace eval getjob {
        variable -nohang off
        variable -timeout -1
        variable -count -1
        variable -withcounters off
    }
    # This holds the defaults options for iterating through queues
    namespace eval qscan {
        variable -count -1
        variable -busyloop off
        variable -minlen -1
        variable -maxlex -1
        variable -importrate -1
    }
    # This holds the defaults options for iterating through jobs
    namespace eval jscan {
        variable -count -1
        variable -busyloop off
        variable -queue ""
        variable -reply ""
        variable -states {}
    }
    
    namespace export {[a-z]*}
    # Create an alias for new as the name of the current namespace, this is the
    # only command that is really exposed.
    interp alias {} [namespace current] {} [namespace current]::new
}


# Implementation notes.
#
# The current behaviour of the dispatch command is to first pick internal lower
# case procedures, then to look for wrapped implementations led by the keyword
# Command and then to let the command be a DISQUE command. This means that the
# "namespace" of internal lower case commands should not overlap with the
# commands that are official DISQUE commands. These are, for now: auth, ping,
# info, shutdown, monitor, debug, config, cluster, client, slowlog, time,
# command, latency, hello, addjob, getjob, ackjob, fastack, deljob, show,
# loadjob, bgrewriteaof, qlen, qpeek, qstat, qscan, jscan, enqueue, dequeue,
# nack, working, pause.
#
# We might want to modify the code so that the list of commands is requested
# from the node on startup, so as to know exactly the subset of commands that we
# can use in the dispatching implementation.


# ::disque::new -- Create a new DISQUE cluster connection
#
#       Creates and connect to one of the DISQUE nodes specified in the
#       arguments. This procedure returns an identifier for the connection, an
#       identifier which also is a command for all further operations with the
#       remote node and internally. This command takes a number of dash-led
#       options and their values that all have good defaults. These options are:
#
#       -nodes     A list of nodes to connect to. A node specification is of the
#                  form password@hostname:port where the password and the port
#                  can be omitted.
#       -port      The default port to use when the port is omitted from a node
#                  specification.
#       -auth      The default password to use when the port is omitted from a node
#                  specification.
#       -liveness  A callback to follow the state of the connection. When called,
#                  this command will take these additional arguments:
#                  The identifier of the connection
#                  An upper case one word description of the state
#                  Additional arguments depending on the state.
#       -grace     Grace period (in ms) when shutting down a node, negative to turn
#                  off.
#       -polldown  Period (in ms) at which to poll for job queues emptiness when
#                  shutting down, negative to turn off.
#
# Arguments:
#	See above
#
# Results:
#       Return an identifier for the connection, this identifier is also a
#       command with which all further operations should be performed.
#
# Side Effects:
#       None.
proc ::disque::new { args } {
    # Create new connection object.
    set d [namespace current]::[incr vars::id]
    upvar \#0 $d D
    
    # Capture arguments and give good defaults into the connection object (a
    # dictionry). Create the command
    repro defaults D disque {*}$args    
    interp alias {} $d {} [namespace current]::Dispatch $d
    
    # Initialise internal state of connection object.
    dict set D sock ""
    
    # Connect. On failure cleanup and return an error. XXX: This should be
    # modified, instead we should try picking another node at regular intervals
    # until we are connected.
    if { [catch {Connect $d} err] } {
        interp alias {} $d {}
        unset $d
        return -code error $err
    }
    
    return $d
}


# ::disque::raw -- Raw DISQUE command
#
#       This sends a raw DISQUE command following the API specification to the
#       remote node that we are connected to, and wait for the answer from the
#       node.
#
# Arguments:
#	d	Identifier of the connection
#	cmd	Command to send (will automatically be uppercased)
#	args	Blind arguments passing to DISQUE node.
#
# Results:
#       Return the answer from the node, or an error.
#
# Side Effects:
#       None.
proc ::disque::raw { d cmd args } {
    upvar \#0 $d D
    
    if { [dict get $D sock] ne "" } {
        Liveness $d COMMAND $cmd $args
        set answer [repro command [dict get $D sock] [string toupper $cmd] {*}$args]
        return $answer
    }
    return -code error "No connection!"
}


# ::disque::close -- Close connection to cluster
#
#       Close the connection to the cluster
#
# Arguments:
#	d	Identifier of the connection
#
# Results:
#       None.
#
# Side Effects:
#       All references to the connection are lost and memory is cleaned up.
proc ::disque::close { d } {
    Liveness $d CLOSE
    repro disconnect $d
    unset $d
    interp alias {} $d {}
}


# ::disque::shutdown -- Gracefull node shutdown
#
#       This will gracefully shutdown the node that this client is connected to.
#       This implements the recommended way of shutting down a node in DISQUE.
#       Note that shutting down will be performed in the background and that the
#       connection should not be used after this point. The "object"
#       representing the connection will automatically disappear once the
#       shutting down procedure has come to an end.
#
# Arguments:
#	d	Identifier of the connection
#
# Results:
#       None.
#
# Side Effects:
#       Timely shutdown the remote node to which we are connected!!
proc ::disque::shutdown { d } {
    upvar \#0 $d D
    # Tell the cluster that we are leaving and possible interested parties.
    raw $d CLUSTER leaving yes
    Liveness $d LEAVING
    
    # Wait for the number of jobs at the nodes to be 0
    if { [dict get $D -grace] > 0 } {
        if { [dict get $D -polldown] > 0 } {
            dict set D leaving [after idle [list [namespace current]::Leave? $d]]
        }
        dict set D timeout [after [dict get $D -grace] [list [namespace current]::Timeout $d]]
    }
}


# ::disque::Shutdown -- Abrubt shutdown
#
#       This will immediately shutdown the node to which this client is
#       connected.
#
# Arguments:
#	d	Identifier of the connection.
#
# Results:
#       None.
#
# Side Effects:
#       None.
proc ::disque::Shutdown { d } {
    upvar \#0 $d D
    catch {raw $d SHUTDOWN}
    Liveness $d SHUTDOWN
    catch {close $d}        
}


# ::disque::Timeout -- Shutdown timeout
#
#       This will get called when we could not wait for the number of jobs for
#       being 0 when shutting down. It forces a shutdown of the node.
#
# Arguments:
#	d	Idnetifier of the connection
#
# Results:
#       None.
#
# Side Effects:
#       None.
proc ::disque::Timeout { d } {
    upvar \#0 $d D
    
    # Cancel any polling for number of jobs.
    if { [dict exists $D leaving] } {
        after cancel [dict get $D leaving]
        dict unset D leaving
    }
    
    # Force a shutdown.
    Shutdown $d
}


# ::disque::Leave? -- Wait for empty job queue
#
#       This will poll for the number of jobs remaining in queues during the
#       shutdown procedure. When there are no more jobs, this will tell all
#       other nodes that this node has gone and then shutdown.
#
# Arguments:
#	d	Identifier of the connection
#
# Results:
#       None.
#
# Side Effects:
#       None.
proc ::disque::Leave? { d } {
    upvar \#0 $d D
    
    # Get number of registered jobs and quit once zero.
    set response [CommandINFO $d jobs]
    if { [dict exists $response registered_jobs] } {
        if { [dict get $response registered_jobs] == 0 } {
            # Remove timeout
            if { [dict exists $D timeout] } {
                after cancel [dict get $D timeout]
                dict unset D timeout
            }
            Liveness $d LEFT
            ### We should send the following command to all OTHER nodes in
            ### cluster, meaning we need to know which they are, through issuing
            ### a new HELLO command.
            ###raw $d CLUSTER forget [dict get $D id]
            Shutdown $d
        }
    }
    dict set D leaving [after [dict get $D -polldown] [list [namespace current]::Leave? $d]]
}


# ::disque::Dispatch -- Dispatch to internal or DISQUE commands
#
#       This will dispatch to both internal and/or DISQUE commands in the
#       following ways. If the command is in lower case and exists as a
#       procedure in this namespace, it will be called. Otherwise, if the
#       keyword Command followed by the uppercased version of the command exists
#       as a procedure in this namespace it will be used. Otherwise, the command
#       is considered to be a DISQUE command and it is sent to the node. This
#       means that there cannot be internal commands that have the same name as
#       the DISQUE commands. Note that this implementation uses tailcall to ease
#       stack tracing.
#
# Arguments:
#	d	Identifier of the connection
#	cmd	Command to execute (see descr)
#	args	Arguments to command, command-dependent.
#
# Results:
#       None.
#
# Side Effects:
#       None.
proc ::disque::Dispatch { d cmd args } {
    # Try finding the command as one of our internally implemented procedures.
    if { [string tolower $cmd] eq $cmd } {
        if { [llength [info commands [namespace current]::$cmd]] } {
            tailcall [namespace current]::$cmd $d {*}$args
        }
    }
    
    # Otherwise convert to upper case and either try the command as a command
    # that we want to wrap internally, or directly as a DISQUE command.
    set cmd [string toupper $cmd]
    if { [llength [info commands [namespace current]::Command$cmd]] } {
        tailcall [namespace current]::Command$cmd $d {*}$args
    } else {
        tailcall [namespace current]::raw $d $cmd {*}$args
    }    
}


# ::disque::Connect -- Connect to a node
#
#       Connect to one of the nodes in the cluster.
#
# Arguments:
#	d	Identifier of the connection.
#
# Results:
#       None.
#
# Side Effects:
#       None.
proc ::disque::Connect { d } {
    upvar \#0 $d D
    
    if { [dict get $D sock] eq "" } {
        # Pick a random node out of the known nodes 
        set len [llength [dict get $D -nodes]]
        set which [expr {int(rand()*$len)}]
        set node [lindex [dict get $D -nodes] $which]
        
        # Parse the node description to collect possible password and port for
        # connection.
        if { [string first @ $node] >= 0 } {
            lassign [split $node @] paswd hstprt
        } else {
            set paswd ""
            set hstprt $node
        }
        lassign [split $hstprt :] hst prt
        
        # Use default password and auth whenever none specified in the node
        # specification.
        if { $prt eq "" } {
            set prt [dict get $D -port]
        }
        if { $paswd eq "" } {
            set paswd [dict get $D -auth]
        }
        
        # Now connect to node
        dict set D sock [repro connect $hst -port $prt -auth $paswd]
        Liveness $d CONNECTED
        
        # And handshake with it for the first time. We do a protocol version
        # check to be sure that we can continue using this API implementation.
        set answer [raw $d "HELLO"]
        set proto [lindex $answer 0]
        if { $proto ne ${vars::-version} } {
            return -code error "Protocol version mismatch when connecting, received: $proto"
        }
        
        # Store the identifier of the node here.
        dict set D id [lindex $answer 1]
        Liveness $d HANDSHAKED
    }
}


# ::disque::jobid -- Analyse Job Identifier
#
#       This will analyse a Job identifier and fail quickly with an error if the
#       string does not comply to the specification.
#
# Arguments:
#	id	Identifier
#
# Results:
#       Return a dictionay containing the various elements of the Job
#       identifier, or an error.
#
# Side Effects:
#       None.
proc ::disque::jobid { id } {
    if { [string length $id] != ${vars::-idlen} } {
        return -code error "$id is not ${vars::-idlen} characters long!"
    }
    lassign [split $id ${vars::-id}] prefix nid id ttl
    # Check DISQUE prefix
    if { $prefix ne "D" } {
        return -code error "$prefix is not the DISQUE prefix"
    }
    # Check the size and formatting of node id and ttl
    foreach {v size} [list nid 8 ttl 4] {
        if { [string length [set $v]] != $size } {
            return -code error "$v is not of length $size"
        }
        set ptn [string repeat {[0-9a-fA-F]} $size]
        if { ![string match $ptn [set $v]] } {
            return -code error "$v is not a properly formated string"
        }
    }
    # We don't do any checking on the id itself, since we've check the total
    # length.
    
    return [dict create -node $nid -id $id -ttl $ttl]
}


proc ::disque::CommandADDJOB { d args } {
    if { [llength $args] < 2 } {
        return -code error "Need at least a queue name and a job content!"
    }
    # Capture queue and body and advance to optional arguments.
    lassign $args queue body
    set args [lrange $args 2 end]
    repro defaults JOB addjob {*}$args
    
    set cmd [list $queue $body]
    Opt2CommandInteger cmd $JOB -timeout ""
    Opt2CommandInteger cmd $JOB -replicate REPLICATE
    Opt2CommandInteger cmd $JOB -delay DELAY
    Opt2CommandInteger cmd $JOB -retry RETRY
    Opt2CommandInteger cmd $JOB -ttl TTL
    Opt2CommandInteger cmd $JOB -maxlen MAXLEN
    Opt2CommandBoolean cmd $JOB -async ASYNC
    set id [raw $d ADDJOB {*}$cmd]
    jobid $id;  # We don't use, but this will verify the ID received
    return $id
}


proc ::disque::CommandGETJOB { d args } {
    set idx [lsearch $args "--"]
    if { $idx >= 0 } {
        set opts [lrange $args 0 [expr {$idx-1}]]
        set args [lrange $args [expr {$idx+1}]]
    } else {
        set opts [list]
        for {set i 0} {$i <[llength $args] } { incr i 2} {
            set opt [lindex $args $i]
            if { [string index $opt 0] eq "-" } {
                lappend opts [lindex $args $i] [lindex $args [expr {$i+1}]]
            } else {
                break
            }
        }
        set args [lrange $args $i end]
    }

    if { [llength $args] == 0 } {
        return -code error "Need at least a queue name!"
    }
    # Capture optional arguments.
    repro defaults GET getjob {*}$opts
    
    set cmd [list]
    Opt2CommandBoolean cmd $GET -nohang NOHANG
    Opt2CommandInteger cmd $GET -timeout TIMEOUT
    Opt2CommandInteger cmd $GET -count COUNT
    Opt2CommandBoolean cmd $GET -withcounters WITHCOUNTERS
    lappend cmd FROM
    set jobs [raw $d GETJOB {*}[linsert $args 0 {*}$cmd]]
    # Properly fail on job IDs that are not...
    foreach job $jobs {
        jobid [lindex $job 1]
    }
    return $jobs
}


proc ::disque::CommandACKJOB { d args } {
    return [CommandJobs $d ACKJOB {*}$args]
}


proc ::disque::CommandFASTACK { d args } {
    return [CommandJobs $d FASTACK {*}$args]
}

proc ::disque::CommandNACK { d args } {
    return [CommandJobs $d NACK {*}$args]
}

proc ::disque::CommandWORKING { d id } {
    return [CommandJobs $d WORKING $id]
}

proc ::disque::CommandENQUEUE { d args } {
    return [CommandJobs $d ENQUEUE {*}$args]
}

proc ::disque::CommandDEQUEUE { d args } {
    return [CommandJobs $d DEQUEUE {*}$args]
}

proc ::disque::CommandDELJOB { d args } {
    return [CommandJobs $d DELJOB {*}$args]
}

proc ::disque::CommandSHOW { d id } {
    return [CommandJobs $d SHOW $id]
}

proc ::disque::CommandJobs { d cmd args } {
    # Check job ids
    foreach id $args {
        jobid $id
    }
    return [raw $d $cmd {*}$args]
    
}


proc ::disque::CommandQSCAN { d cursor args } {
    repro defaults SCAN qscan {*}$args
    
    set cmd [list $cursor]
    Opt2CommandInteger cmd $SCAN -count COUNT
    Opt2CommandBoolean cmd $SCAN -busyloop BUSYLOOP
    Opt2CommandInteger cmd $SCAN -minlen MINLEN
    Opt2CommandInteger cmd $SCAN -maxlen MAXLEN
    Opt2CommandInteger cmd $SCAN -importrate IMPORTRATE
    return [raw $d QSCAN {*}$cmd]
}


proc ::disque::CommandJSCAN { d cursor args } {
    repro defaults SCAN jscan {*}$args
    
    set cmd [list $cursor]
    Opt2CommandInteger cmd $SCAN -count COUNT
    Opt2CommandBoolean cmd $SCAN -busyloop BUSYLOOP
    Opt2CommandString cmd $SCAN -queue QUEUE
    if { [dict exists $SCAN -reply] } {
        if { [dict get $SCAN -reply] ni [list all id] } {
            return -code error "Reply [dict get $SCAN -reply] not a proper reply"
        }
        Opt2CommandString cmd $SCAN -reply REPLY
    }
    if { [dict exists $SCAN -states] } {
        foreach s [dict get $SCAN -states] {
            lappend cmd STATE $s
        }
    }
    return [raw $d JSCAN {*}$cmd]
}


proc ::disque::CommandINFO { d {section "default" } } {
    set lines [raw $d INFO $section]
    set d [dict create]
    foreach l [split $lines "\n"] {
        set l [string trim $l]
        if { $l ne "" && [string index $l 0] ne "\#" } {
            set colon [string first ":" $l]
            dict set d [string range $l 0 [expr {$colon-1}]] [string range $l [expr {$colon+1}] end]
        }
    }
    return $d
}


proc ::disque::CommandPAUSE { d queue args } {
    foreach k $args {
        if { $k ni [list in out all none state bcast] } {
            return -code error "$k is not a recognised paused state"
        }
    }
    return [raw $d PAUSE $queue {*}$args]
}


proc ::disque::Opt2CommandBoolean { argv_ D opt cmd } {
    upvar $argv_ argv
    if { [string is true -strict [dict get $D $opt]] } {
        lappend argv $cmd
    }
}

proc ::disque::Opt2CommandInteger { argv_ D opt cmd } {
    upvar $argv_ argv
    if { [dict get $D $opt] >= 0 } {
        if { ! [string is integer -strict [dict get $D $opt]]} {
            return -code error "$opt is not an integer!"
        }
        if { $cmd ne "" } {
            lappend argv $cmd
        }
        lappend argv [dict get $D $opt]
    }    
}

proc ::disque::Opt2CommandString { argv_ D opt cmd } {
    upvar $argv_ argv
    if { [dict get $D $opt] >= 0 } {
        if { $cmd ne "" } {
            lappend argv $cmd
        }
        lappend argv [dict get $D $opt]
    }    
}


# ::disque::Liveness -- Connection liveness callback
#
#       Callback as the state of the connection progresses so that external
#       callers can take decisions.
#
# Arguments:
#	d	Identifier of the conneciton.
#	state	Current state
#	args	Additional arguments to state, dependent on state.
#
# Results:
#       None.
#
# Side Effects:
#       Will log errors, nothing more.
proc ::disque::Liveness { d state args } {
    upvar \#0 $d D
    
    if { [llength [dict get $D -liveness]] } {
        if { [catch {{*}[dict get $D -liveness] $d [string toupper $state] {*}$args} err] } {
            Log WARN "Could not callback for liveness: $err"
        }
    }
}


proc ::disque::Log { d lvl msg } {
    set lvl [string tolower $lvl]
    puts stderr "\[$lvl\] $msg"
}