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

proc ::disque::new { args } {
    set d [namespace current]::[incr vars::id]
    upvar \#0 $d D
    
    repro defaults D disque {*}$args    
    interp alias {} $d {} [namespace current]::Dispatch $d
    
    dict set D sock ""
    if { [catch {Connect $d} err] } {
        interp alias {} $d {}
        unset $d
        return -code error $err
    }
    
    return $d
}


proc ::disque::command { d cmd args } {
    upvar \#0 $d D
    
    if { [dict get $D sock] ne "" } {
        Liveness $d COMMAND $cmd $args
        set answer [repro command [dict get $D sock] [string toupper $cmd] {*}$args]
        return $answer
    }
    return -code error "No connection!"
}


proc ::disque::close { d } {
    Liveness $d CLOSE
    repro disconnect $d
    unset $d
    interp alias {} $d {}
}


proc ::disque::shutdown { d } {
    upvar \#0 $d D
    command $d CLUSTER leaving yes
    Liveness $d LEAVING
    if { [dict get $D -grace] > 0 } {
        if { [dict get $D -polldown] > 0 } {
            dict set D leaving [after idle [list [namespace current]::Leave? $d]]
        }
        dict set D timeout [after [dict get $D -grace] [list [namespace current]::Timeout $d]]
    }
}


proc ::disque::Shutdown { d } {
    upvar \#0 $d D
    catch {command $d SHUTDOWN}
    Liveness $d SHUTDOWN
    catch {close $d}        
}


proc ::disque::Timeout { d } {
    upvar \#0 $d D
    if { [dict exists $D leaving] } {
        after cancel [dict get $D leaving]
        dict unset D leaving
    }
    Shutdown $d
}


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
            ###command $d CLUSTER forget [dict get $D id]
            Shutdown $d
        }
    }
    dict set D leaving [after [dict get $D -polldown] [list [namespace current]::Leave? $d]]
}


proc ::disque::Dispatch { d cmd args } {
    if { [string tolower $cmd] eq $cmd } {
        if { [llength [info commands [namespace current]::$cmd]] } {
            tailcall [namespace current]::$cmd $d {*}$args
        }
    }
    
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


proc ::disque::CommandQSCAN { d cursor args } {
    repro defaults SCAN qscan {*}$args
    
    set cmd [list $cursor]
    Opt2CommandInteger cmd $SCAN -count COUNT
    Opt2CommandBoolean cmd $SCAN -busyloop BUSYLOOP
    Opt2CommandInteger cmd $SCAN -minlen MINLEN
    Opt2CommandInteger cmd $SCAN -maxlen MAXLEN
    Opt2CommandInteger cmd $SCAN -importrate IMPORTRATE
    return [command $d QSCAN {*}$cmd]
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
    return [command $d JSCAN {*}$cmd]
}


proc ::disque::CommandINFO { d {section "default" } } {
    set lines [command $d INFO $section]
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
    return [command $d PAUSE $queue {*}$args]
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