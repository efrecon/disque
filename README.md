# DISQUE Tcl

These two Tcl modules implements a high-level interface to the
[DISQUE](https://github.com/antirez/disque) distributed message broker. The
modules abstracts most of the API, as examplified in the following code snippet.

    package require disque
    set d [disque -nodes localhost];           # Connect to localhost only.
    set id [$d addjob /queue test];            # Enqueue a job
    lassign [$d getjob /queue] queue id body;  # Get it back
    
The programming interface for the main `disque` module happens through a single
command called `disque`, which will connect randomly to one of the nodes
specified as part of its arguments and return an identifier for the connection.
The remaining of the programming interface is loosely based on Tk, meaning that
the identifier returned after connection is also a command with a number of
sub-commands to communicate with the DISQUE node and/or close the connection.

The module automatically provides a wrapper for any DISQUE command, thus
providing forward compatibility as the API will (?) evolve. However, most of
the DISQUE API has been wrapped through Tcl-friendly commands.