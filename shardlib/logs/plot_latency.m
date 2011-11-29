#!/usr/bin/octave
args = argv();

%% process arguments
max_argc = 7;
opt_argc = 2;

if length(args) < (max_argc - opt_argc)
printf("usage: <script> systemparams graphparams workloadparams xlabel title maxx maxy\n");
printf("NOTE: the first word of the title also goes to the output filename\n");
quit;
end

system = args{1};
graph = args{2};
workload = args{3};
xlabel = args{4}
title = args{5};

x = fscanf(stdin, "%d");
x = x/1000;

if length(args) < max_argc
maxfreq = length(x);
maxnumber = max(x);
maxy = maxfreq*0.5;
maxx = 1.2*maxnumber;
else
maxx = sscanf(args{max_argc - 1}, "%d");
maxy = sscanf(args{max_argc}, "%d");
end

%%
%get quantiles before zooming in
quantiles = [0, 50, 90, 99, 99.9, 99.99, 100]/100;
stats = quantile(x, quantiles);

quantilestring = sprintf("%.1f %.1f %.1f %.2f %.3f %.4f %.4f", quantiles);
statstring = sprintf("%.1f %.1f %.1f %.1f %.1f %.1f %.1f", stats);

disp(['quantiles: ', quantilestring])
disp(['values: ', statstring]);

%select only desired range
x = x(find(x < maxx));
hist(x,50);
ylabel('frequency');
xlabel(xlabel);
title(title, 'fontsize', 10);

miny = -0.3*maxy; 
minx = -0.1*maxx; 

xlim([minx, maxx]);
ylim([miny, maxy]);

step = 0.025*(maxy - miny);
text(0.5*maxx, 0.5*maxy + 0*step, quantilestring);
text(0.5*maxx, 0.5*maxy + 1*step, statstring);

show_parameters(system, graph, workload)
graph_params = strsplit(graph, ' ', true);
print([title{1}, graph_params{end}, '.pdf'], '-dpdf', '-landscape')

sleep(0.01); 
quit;
