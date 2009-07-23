Cases
=====

  0. Prime number check

  1. Pairwise structure comparison using DaliLite as provided by the web-service
     from EBI (http://www.ebi.ac.uk/Tools/webservices/clients/dalilite) and the
     provided perl script. This example shows how to re-use instead of rewriting
     by wrapping existing code in a piper.

  2. Not all pipelines are deterministic, this use-case shows how to incorporate
     repetitions of randomized functions for a small genome we calculate the
     Z-score, that the free energy of RNA folding is different from what is
     expected from the dinucleotide distribution.

  3. Calculating an all-vs-all RMSD can be time and memory consuming, this is
     our memory-efficient and parallel approach 

  4. When one computer is not enough. Creating your own papy botnet using RPyC
     techonology.
