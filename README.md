# DISQUE Tcl

These two Tcl modules implements a high-level interface to the
[DISQUE](https://github.com/antirez/disque) distributed message broker. The
modules abstracts most of the API, as examplified in the following code snippet.

    package require disque
    set d [disque -nodes localhost];           # Connect to localhost only.
    set id [$d addjob -async /queue test];     # Enqueue a job
    lassign [$d getjob /queue] queue id body;  # Get it back
    $d ackjob $id;                             # ACK it as done.
    
The programming interface for the main `disque` module happens through a single
command called `disque`, which will connect randomly to one of the nodes
specified as part of its arguments and return an identifier for the connection.
The remaining of the programming interface is loosely based on Tk, meaning that
the identifier returned after connection is also a command with a number of
sub-commands to communicate with the DISQUE node and/or close the connection.

The module automatically provides a wrapper for any DISQUE command, thus
providing forward compatibility as or if the API will evolve. However, most of
the DISQUE API has been wrapped through Tcl-friendly commands. Most of the
wrapping involves the introduction of dash-led options that will directly map to
the optional parameters of the DISQUE API. For example, calling the following:

    $d addjob -async -replicate 3 /queue test
    
would lead to sending the following DISQUE command to the node:

    ADDJOB /queue test 1000 REPLICATE 3 ASYNC
    
Note that, in that very special case, a default timeout of `1000` is automatically
added, which can be otherwise controlled by the `-timeout` option. To this
regard, the ADDJOB implementation is the only one that deviates from this
1-to-1 mapping between the optional paramaters of DISQUE and the dash-led
options. This decision was taken for the command to better respect the Tcl
conventions. Note that the options are placed directly after the name of the
command, again to better respect the Tcl coding conventions.