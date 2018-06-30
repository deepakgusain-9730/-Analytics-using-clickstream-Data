A= load 'clickstream/omniture.0.tsv' using PigStorage('\t');
 B = foreach A generate $0,$1,$7,$12,$13,$27,$38,$39,$43,$49,$50,$51,$52;
  store B into 'clickstreamforhive/ominutre.txt';

