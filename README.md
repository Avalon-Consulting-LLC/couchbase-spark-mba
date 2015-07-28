# Spark - Couchbase Market Basket Analysis
This repository is companion code for a blog post on our site: http://blogs.avalonconsult.com/blog/big-data/combining-operational-and-analytical-big-data-using-couchbase-and-spark-a-market-basket-analysis-example/.

This project is an all in one environment that setups a Vagrant machine with Couchbase and Spark installed. And has a Spark process that will generate some basic Market Basket Analytics in the form of product recommendations.

Sample data taken from:

	Brijs T., Swinnen G., Vanhoof K., and Wets G. (1999), The use of association rules for product
	assortment decisions: a case study, in: Proceedings of the Fifth International Conference on
	Knowledge Discovery and Data Mining, San Diego (USA), August 15-18, pp. 254-260. ISBN:
	1-58113-143-7.
	
Can be found under the 'retail' link here: http://fimi.ua.ac.be/data/

Prerequisites
-------------
1. Install Virtualbox: https://www.virtualbox.org/wiki/Downloads

2. Install Vagrant: http://www.vagrantup.com/downloads.html

3. Install necessary Vagrant plugins:

```sh
vagrant plugin install vagrant-hostmanager
vagrant plugin install vagrant-cachier
```

4. Install Ansible

```sh
brew install ansible
```

Getting Started
------
Start by bringing up the Vagrant machine, it is configured to install everything you need to run the analysis

```sh
cd vagrant
vagrant up
```

Load the sample data by SSHing into the machine and running the tocb.py script.

```sh
cd vagrant
vagrant ssh
cd /vagrant
python tocb.py
```

At the top level of the repo, go into the Spark project, build it, then move the jar file into the Vagrant shared folder:

```sh
cd mba
mvn clean package
cp target/mba-1.0-SNAPSHOT.jar ../vagrant/
```

Once the jar is in place you can SSH into the vagrant machine and run the process:

```sh
cd vagrant
vagrant ssh
/opt/spark-1.4.1-bin-hadoop2.6/bin/spark-submit --class com.avalonconsult.mba.MBA /vagrant/mba-1.0-SNAPSHOT.jar
```

You can access the Couchbase UI at retail.vagrant:8091 with credentials: couchbase//couchbase