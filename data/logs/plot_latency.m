#!/usr/bin/octave
args = argv();

%% process arguments
max_argc = 7;
opt_argc = 2;

if length(args) < (max_argc - opt_argc)
printf("usage: <script> systemparams graphparams workloadparams xlabelval titleval maxx maxy\n");
printf("NOTE: the first word of the titleval also goes to the output filename\n");
quit;
end

system = args{1};
graph = args{2};
workload = args{3};
xlabelval = args{4}
titleval = args{5};

x = fscanf(stdin, "%d");

if length(args) < max_argc
  maxfreq = length(x);
  maxnumber = max(x);
  maxy = maxfreq*0.5;
  maxx = 1.2*maxnumber;
else
  maxx = sscanf(args{max_argc - 1}, "%d");
  maxy = sscanf(args{max_argc}, "%d");
end

%get quantiles before zooming in
quantiles = [50, 90, 99, 99.9]/100;

disp(['num of zero valued entries:' , sprintf("%d", sum(x(x==0)))])
x = x(x>0);
stats = quantile(x, quantiles);
joint = [quantiles; stats'];

totaln = length(x);

x = x(find(x < maxx));
disp(['fracton of points included in plot window: ', sprintf("%d/%d", length(x),totaln)])

assert(size(x,1) > 0, 'too few values in window. modify maxx param');

hist(x, 50, 100);
ylabel('frequency (\%)');
xlabel("latency (micro sec)");
title(titleval, 'fontsize', 10);

miny = -0.3*maxy; 
minx = -0.1*maxx; 

xlim([minx, maxx]);
ylim([miny, maxy]);

step = 0.025*(maxy - miny);

text(0.4*maxx, 0.7*maxy + 1*step, sprintf("n = %d (%.0f%% displayed)", totaln, 100*length(x)/totaln));
text(0.4*maxx, 0.7*maxy - 0*step, 'quantiles:');
for i=1:size(joint,2)
  quantilestring = sprintf("%.3f : %.0f", joint(:,i));
  text(0.4*maxx, 0.7*maxy - (i)*step, quantilestring);
end 

#show_parameters(system, graph, workload)
graph_params = strsplit(graph, ' ', true);
title_first = strsplit(titleval, ' ', true){1};
print([title_first, '.png'], '-dpng', '-landscape')

sleep(30); 
quit;
