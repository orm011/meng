#!/usr/bin/octave

% numbers are as microseconds, 
% need to count and then divide by number of seconds in interval
if length(argv) < 2
     printf('usage: <script> datafile titlestring');
     quit;
end

args = argv();
filename = args{1};
titlestring = args{2};

x = fscanf(fopen(filename), "%d");
x = x - min(x);
[yc, xc] = hist(x);

width = xc(2) - xc(1);
yc = yc*(1e6/width);

plot(xc, yc, "rxo"); 

temp = ylim();
temp(1)= 0;

ylabel('requests per second');
xlabel('elapsed time in musec');
title(titlestring);
ylim(temp);

sleep(0.01); 
quit;
