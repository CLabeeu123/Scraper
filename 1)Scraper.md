# Scraper
Scraper which outputs the most valuable Hash for Bitcoin per minute. From the site (https://www.blockchain.com/btc/unconfirmed-transactions). Based on this data, we will determine the most valuable Hash for Bitcoin per minute.
We will be working on a Ubuntu machine and also with python.
<br>

<h1>To install</h1>
<h3>Needed libraries in python , we will install them later</h3>
BeautifulSoup, Requests, Pandas, time and logging
<h3>A Ubuntu VM </h3>
<p>Does not matter wich version</p>

<h1>Get your Ubuntu running with these commands</h1>
<ul>
<li>sudo apt update</li>
<li>sudo apt install software-properties-common</li>
<li>sudo add-apt-repository ppa:deadsnakes/ppa</li>
<li>sudo apt update</li>
<li>sudo apt install python3</li>
<li>sudo apt install python3-pip</li>
</ul>

<h1>Clone the git and give accces</h1>
<p>First we need to clone the repository to acces it ourselfs using these commands:</p>

<p>git clone https://github.com/CLabeeu123/Scraper.git</p>
<p>cd Scraper</p>

<p>Allow the tool to acces your path:</p>

<p>chmod +x Scraper.py</p>

<h3>Install the needed libraries in your file</h3>
<ul>
  <li>pip3 install BeautifulSoup4</li>
  <li>pip3 install Pandas</li>
  <li>pip3 install Requests</li>
  <li>pip3 install bs4</li>
  <li>pip3 install logging (might already be installed)</li>
  </ul>

<h1>Run the code</h1>

<p>Next up if you want to run it:</p>
<p>python3 ScraperChristophe.py</p>
<h3>This code will make 2 log files</h3>
<p>To open them you can either go in your Scraper file or open them in the terminal</p>
<p>To open in terminal use cat NameOfFile.log

<h1>Code</h1>
<h3>Incase you want some more info about the code </h3>
<p>You can find some comments inside the code that explains what it does and how it works</p>



