fanouts = load 'fanouts-per-user';
numusers = foreach (group fanouts all) generate COUNT(fanouts);
store numusers into 'fanouts-per-user-counts';