# Redis
We have learned however that Redis can be used as a caching mechanism!
We will be working on a Ubuntu machine and also with python.
<br>
<h1>NOTE DO NOT FORGET TO PULL THE UPDATED CODE FIRST IN YOUR VM, EXPLAINED BELOW</h1>

<h1>To install</h1>
<h3>Extra library you need to install</h3>
  Multiprocessing and Redis
<h3>A Ubuntu VM </h3>
<p>We will be using the same one as last time</p>

<h1>Redis</h1>
<h3>Run the Redis bash file</h3>
<ul>
 <li>cd Scraper</li>
 <li>chmod +x redis.sh</li>
 <li>redis.sh</li>
</ul>
<p>Your Redis should be up and running</p>

<h1>Update the git and give accces</h1>
<p>First we need to update our code so we will need the new code in our VM:</p>

<p>git fetch origin</p>
<p>git commit -m 'Commit'</p>
<p>Press ctrl + x</p>
<p>git pull</p>
<p>cd Scraper</p>

<p>Allow the tool to acces your path:</p>

<p>chmod +x toDB.py</p>
<p>chmod +x redConnectMongo.py</p>

<h3>Install the needed libraries in your file</h3>
<ul>
  <li>pip3 install Redis</li>
  <li>pip3 install Multiprocessing</li>
  </ul>

<h1>Run the code</h1>

<p>Next up if you want to run it:</p>
<p>python3 toDB.py</p>
<p>Then we need to run the redConnectMongo.py</p>

<h3>Next up open the mongo compass and see the changes </h3>

<h1>Code</h1>
<h3>Incase you want some more info about the code </h3>
<p>You can find some comments inside the code that explains what it does and how it works</p>

