netbps:
https://superuser.com/questions/356907/how-to-get-real-time-network-statistics-in-linux-with-kb-mb-bytes-format-and-for
command:
sudo tcpdump -i eth0 -l -e -n "port 2018 or port 5000 or 2017 or 2020 or 2010 or 2011 or 2012" | ./netbps.perl
