#!/usr/bin/env perl

sub usage {
    warn <<EOF;
$0 [ 'inserts' | 'replaces' | 'upserts' ] [ batch_size ] [ total_docs ]

    numeric args are: batch_size if number <= 1000, else total_docs
EOF
    warn "$_[0]\n" if @_;
}

use Modern::Perl;
use MongoDB;
use Data::Dumper;
use Try::Tiny;
use Time::Piece;

my @batch       = ();
my $total_docs  = 5_000_000;
my $batch_size  = 1000;         # cmdline args <= 1000 set this (0 stops batching)
my $total_ops   = 0;
my $total_mods  = 0;
my $total_ins   = 0;
my $total_ups   = 0;
my $doing       = 'upserts';

while (@ARGV) {
    my $arg = shift;
    if ($arg =~ /^(inserts|replaces|upserts)$/) { $doing = $arg;
    } elsif ($arg =~ /^\d+$/)                   { if ($arg > 1000) { $total_docs = $arg; } else { $batch_size = $arg; }
    } elsif ($arg eq '-h' or $arg eq '--help')  { usage; exit 0;
    } else                                      { usage "Bad arg: $arg"; exit 2;
    }
}
my $report_each = $total_docs/10; $report_each = 250_000 if $report_each > 250_000;

########################################

=pod

sub do_batch {
    my($batch, $coll) = @_;
    return unless @$batch;
    my $res = $coll->bulk_write( [ @$batch ], { ordered => 0 }); # upsert => 1 } );
    say Dumper $res if @{ $res->write_errors // [] };
    $total_ops += $res->deleted_count;
    $total_ins += $res->inserted_count;
    $total_ups += $res->upserted_count;
    $total_mods += $res->modified_count;
    @$batch = ();
}

=cut

sub do_bulk {
    my($bulk_r, $coll) = @_;
    # warn __LINE__, ' bulked ', $$bulk_r->_count_writes, ' execed ', $$bulk_r->_executed, "\n";
    return unless $$bulk_r->_count_writes;
    my $res = $$bulk_r->execute;
    say Dumper $res if @{ $res->write_errors // [] };
    $total_ops += $res->deleted_count;
    $total_ins += $res->inserted_count;
    $total_ups += $res->upserted_count;
    $total_mods += $res->modified_count;
    $$bulk_r = $coll->initialize_unordered_bulk_op;
    # warn __LINE__, ' bulked ', $$bulk_r->_count_writes, ' execed ', $$bulk_r->_executed, "\n";
}

sub timestamp {
    my($doing, $docs_done, $batch_size, $last_report_r, $start_time) = @_;
    my $elapsed_time = ($$last_report_r = time)-$start_time;
    say localtime->datetime . " pl $doing $docs_done / $total_docs ($batch_size per batch) in ${elapsed_time} s = ", int(10*$docs_done/($elapsed_time || 1))/10, "/s ops:$total_ops mods:$total_mods ins:$total_ins ups:$total_ups";
}

########################################

my $start_time = time; my $last_report = $start_time;
say localtime->datetime . " pl $doing 0 / $total_docs ($batch_size per batch) start_time=$start_time";
try {
    my $db = MongoDB->connect() or die "No connect";
    my $coll = $db->ns('dbsoak.pl'); # database.collection
    my $bulk;
    if ($batch_size) {
        $bulk = $coll->initialize_unordered_bulk_op;
    }

    my $res;
    for my $doc_num (1..$total_docs) {
        my $myid  = $doc_num % 1000;
        if ($batch_size) {
            if       ($doing eq 'upserts') { $bulk->find({ myid => $myid })->upsert->update_one({ '$set' => { myval => $doc_num, timestamp => localtime->datetime } });
            } elsif ($doing eq 'replaces') { $bulk->find({ myid => $myid })->upsert->replace_one({ myid => $myid, myval => $doc_num, timestamp => localtime->datetime });
            } else                         { $bulk->insert_one({ myid => $myid, myval => $doc_num, timestamp => localtime->datetime });
            # if       ($doing eq 'upserts') { push @batch, update_one  => [ { myid => $myid }, { '$set' => { myval => $doc_num } }, { upsert => 1 } ];
            # } elsif ($doing eq 'replaces') { push @batch, replace_one => [ { myid => $myid }, { myval => $doc_num }, { upsert => 1 } ];
            # } else                         { push @batch, insert_one  => [ { myid => $myid, myval => $doc_num } ];
            }
            # do_batch(\@batch, $coll) if @batch >= $batch_size;
            do_bulk(\$bulk, $coll) if $doc_num%$batch_size == 0;
        } else {
            if       ($doing eq 'upserts') { $res = $coll->update_one( { myid => $myid } , { '$set' => { myval => $doc_num, timestamp => localtime->datetime } }, { upsert => 1 } );
            } elsif ($doing eq 'replaces') { $res = $coll->replace_one( { myid => $myid } , { myval => $doc_num, timestamp => localtime->datetime }, { upsert => 1 } );
            } else                         { $res = $coll->insert_one( { myid => $myid, myval => $doc_num, timestamp => localtime->datetime } );
            }
            say Dumper $res if @{ $res->write_errors // [] };
        }
        timestamp($doing, $doc_num, $batch_size, \$last_report, $start_time) if $doc_num % $report_each == 0 or time > 30+$last_report;
    }
    # do_batch(\@batch, $coll);
    do_bulk(\$bulk, $coll) if $batch_size;
    timestamp($doing, $total_docs, $batch_size, \$last_report, $start_time);
} catch {
    die "ERROR ", ref($_), ": ", (ref($_) ? $_->message() : $_);
}
