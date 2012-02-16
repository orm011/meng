#!/usr/bin/octave

args = argv();

if length(args) < 3
printf('usage: <script> systemparams graphparams workloadparams');
quit;
end

system = args{1};
graph = args{2};
workload = args{3};

x = fscanf(stdin, "%d");
x = x/1000;

hist(x,100);
ylabel('frequency');
xlabel('degree');
title(['degree distribution'], 'fontsize', 10);

maxfreq = length(x);
maxy = maxfreq*0.5;
miny = -0.3*maxy; 

maxnumber = max(x);
maxx = 1.2*maxnumber;
minx = -0.1*maxx; 

xlim([minx, maxx]);
ylim([miny, maxy]);

show_parameters(system, graph, workload)
graph_params = strsplit(graph, ' ', true);
print(['degree', graph_params{end}, '.pdf'], '-dpdf', '-landscape')

sleep(0.01); 
quit;
