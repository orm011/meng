%octave script to generate the plot of latency vs. skew given some files.
%commands used to generate input data from logs are: (need to change to $11 for degree  in GraphParam)
%rm *.temp *.skews
%for i in $(ls); do cat $i | grep Metrics | awk '{print $7}' > $i.temp; done  
%for i in $(ls); do cat $i | grep GraphParameters | awk '{print $NF}' > $i.skews; done
%rm *.temp.skews


path=argv(){1};

lats = ls([path, "*.temp"])
sks = ls([path, "*.nodes"])

assert(size(lats,1) == size(sks,1),"number of files does not match") %sanity

quantile_positions = [0.5, 0.9, 0.99, 0.999, 0.9999]; 
stats = zeros(size(lats,1), length(quantile_positions));
skew_vals = zeros(size(lats,1),1);

%example run for first file

for i=1:size(lats,1)
  fopen(lats(i,:),"r");
  dta = fscanf(fopen(lats(i,:), "r"), "%d");
  stats(i,:) = quantile(dta, quantile_positions);
  skew_vals(i,:) = fscanf(fopen(sks(i,:), "r"), "%d"); %remember to modify for skew
end

disp(stats)
disp(skew_vals)

data = sortrows([skew_vals, stats], 1)

hold on;
for i=2:size(data,2)
	plot(data(:,1), data(:,i), 'o-')
end

xlabel('nodes in system'); %nodes
ylabel('latency in micro sec');

sleep(1);
