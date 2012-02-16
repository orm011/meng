function show_parameters(system, graph, workload)
%depends on axes state

xlimits = xlim();
ylimits = ylim();

minx = xlimits(1);
maxx = xlimits(2);

miny = ylimits(1);
maxy = ylimits(2);

textX = minx + 0.02*(maxx - minx);
textY = miny + 0.02*(maxy - miny);
step = 0.035*(maxy - miny);

text(textX, textY + 4*step, system);
text(textX, textY + 3*step, graph(1:length(graph)/2));
text(textX, textY + 2*step, graph(ceil(length(graph)/2):end));
text(textX, textY + step, workload(1:length(workload)/2));
text(textX, textY, workload(ceil(length(workload)/2):end));
end
