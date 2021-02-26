# MongoDb
Now that we have scraped our website we want to put our precious data into a DataBase.
We will be working on a Ubuntu machine and also with python.
<br>
<h1>NOTE DO NOT FORGET TO PULL THE UPDATED CODE FIRST IN YOUR VM, EXPLAINED BELOW</h1>

<h1>To install</h1>
<h3>Extra library you need to install</h3>
  pymongo
<h3>A Ubuntu VM </h3>
<p>We will be using the same one as last time</p>

<h1>MongoDB</h1>
<p>Download the mongoDB and the mongo Compass for a UI.</p>
<h4>The commands you will need to create your mongoDB</h3>
<ul>
 <li>sudo apt install curl</li>
<li>curl -fsSL https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -</li>
<li>echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list</li>
<li>sudo apt update</li>
<li>sudo apt install mongodb-org</li>
<li>sudo systemctl start mongod.service</li>
<li>sudo systemctl status mongod</li>
<li>sudo systemctl enable mongod</li>
<li>mongo --eval 'db.runCommand({ connectionStatus: 1 })'</li>
</ul>
<h3>Or run the bash file</h3>
<ul>
 <li>cd Scraper</li>
 <li>chmod +x mongo.sh</li>
 <li>mongo.sh</li>
</ul>

<h1>Update the git and give accces</h1>
<p>First we need to update our code so we will need the new code in our VM:</p>

<p>git fetch origin</p>
<p>git commit -m 'Commit'</p>
<p>Press ctrl + x</p>
<p>git pull</p>
<p>cd Scraper</p>

<p>Allow the tool to acces your path:</p>

<p>chmod +x MongoDB.py</p>

<h3>Install the needed libraries in your file</h3>
<ul>
  <li>pip3 install pymongo</li>
  </ul>

<h1>Run the code</h1>

<p>Next up if you want to run it:</p>
<p>python3 mongoDB.py</p>
<p>This will forward the output to your mongo database.

<h1>Code</h1>
<h3>Incase you want some more info about the code </h3>
<p>You can find some comments inside the code that explains what it does and how it works</p>
