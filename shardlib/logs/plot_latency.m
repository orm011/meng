#!/usr/bin/octave

args = argv();

if length(args) < 2
printf('usage: <script> datafile titlestring');
quit;
end

filename = args{1};
titlestring = args{2};

x = fscanf(fopen(filename), "%d");
x = x/1000;

hist(x,100);
ylabel('frequency');
xlabel('latency in musec');
title(['latency distribution. n = ', sprintf("%d", length(x)), " ", titlestring], 'fontsize', 5);

sleep(0.01); 
quit;
