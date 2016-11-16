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
    namespace eval getjob {
        variable -nohang off
        variable -timeout -1
        variable -count -1
        variable -withcounters off
    }
    
    namespace export {[a-z]*}
    # Create an alias for new as the name of the current namespace, this is the
    # only command that is really exposed.
    interp alias {} [namespace current] {} [namespace current]::new
}

proc ::disque::new { args } {
    set d [namespace current]::[incr vars::id]
    upvar \#0 $d D
    
    repro defaults D disque {*}$args    
    interp alias {} $d {} [namespace current]::Dispatch $d
    
    dict set D sock ""
    Connect $d;  # XXX: Catch, capture error, cleanup and return error
    
    return $d
}


proc ::disque::command { d cmd args } {
    upvar \#0 $d D
    
    if { [dict get $D sock] ne "" } {
        set answer [repro command [dict get $D sock] [string toupper $cmd] {*}$args]
        return $answer
    }
    return -code error "No connection!"
}


proc ::disque::Dispatch { d cmd args } {
    set cmd [string toupper $cmd]
    if { [llength [info commands [namespace current]::Command$cmd]] } {
        tailcall [namespace current]::Command$cmd $d {*}$args
    } else {
        tailcall [namespace current]::command $d $cmd {*}$args
    }
}


proc ::disque::Connect { d } {
    upvar \#0 $d D
    
    if { [dict get $D sock] eq "" } {
        set len [llength [dict get $D -nodes]]
        set which [expr {int(rand()*$len)}]
        set node [lindex [dict get $D -nodes] $which]
        
        if { [string first @ $node] >= 0 } {
            lassign [split $node @] paswd hstprt
        } else {
            set paswd ""
            set hstprt $node
        }
        lassign [split $hstprt :] hst prt
        if { $prt eq "" } {
            set prt [dict get $D -port]
        }
        if { $paswd eq "" } {
            set paswd [dict get $D -auth]
        }
        
        dict set D sock [repro connect $hst -port $prt -auth $paswd]
        Liveness $d CONNECTED
        
        set answer [command $d "HELLO"]
        set proto [lindex $answer 0]
        if { $proto ne ${vars::-version} } {
            return -code error "Protocol version mismatch when connecting, received: $proto"
        }
        
        dict set D id [lindex $answer 1]
        Liveness $d HANDSHAKED
    }
}


proc ::disque::jobid { id } {
    if { [string length $id] != ${vars::-idlen} } {
        return -code error "$id is not ${vars::-idlen} characters long!"
    }
    lassign [split $id ${vars::-id}] prefix nid id ttl
    if { $prefix ne "D" } {
        return -code error "$prefix is not the DISQUE prefix"
    }
    if { ![string match {[0-9a-f][0-9a-f][0-9a-f][0-9a-f]} $ttl] } {
        return -code error "TTL $ttl is not a properly formated Job TTL"
    }
    
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
    set id [command $d ADDJOB {*}$cmd]
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
    set jobs [command $d GETJOB {*}[linsert $args 0 {*}$cmd]]
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
    return [command $d $cmd {*}$args]
    
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