
<!-- This is the project specific website template -->
<!-- It can be changed as liked or replaced by other content -->

<?php

$domain=ereg_replace('[^\.]*\.(.*)$','\1',$_SERVER['HTTP_HOST']);
$group_name=ereg_replace('([^\.]*)\..*$','\1',$_SERVER['HTTP_HOST']);
$themeroot='r-forge.r-project.org/themes/rforge/';

echo '<?xml version="1.0" encoding="UTF-8"?>';
?>
<!DOCTYPE html
	PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
	"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en   ">

  <head>
	<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
	<title><?php echo $group_name; ?></title>
	<link href="http://<?php echo $themeroot; ?>styles/estilo1.css" rel="stylesheet" type="text/css" />
  </head>

<body>

<!-- R-Forge Logo -->
<table border="0" width="100%" cellspacing="0" cellpadding="0">
<tr><td>
<a href="http://r-forge.r-project.org/"><img src="http://<?php echo $themeroot; ?>/imagesrf/logo.png" border="0" alt="R-Forge Logo" /> </a> </td> </tr>
</table>


<!-- get project title  -->
<!-- own website starts here, the following may be changed as you like -->

 <?php if ($handle=fopen('http://'.$domain.'/export/projtitl.php?group_name='.$group_name,'r')){
$contents = '';
while (!feof($handle)) {
	$contents .= fread($handle, 8192);
}
fclose($handle);
echo $contents; } ?>

<!-- end of project description -->

  <p> This project has two packages: sqlsurvey, for analysis of large surveys, and RMonetDB, used by sqlsurvey to communicate with the MonetDB database. RMonetDB is a slight modification of Simon Urbanek's RJDBC to handle some idiosyncracies in MonetDB </p>

   <p> Both packages require <a href="http://www.monetdb.org/Downloads?x=144&y=18">MonetDB</a>. Under Windows it is important to use a 64-bit version of MonetDB to allow creation of large databases. </p>

 Useful notes:
<ul>
   <li> While it is possible to read data into R and then save into MonetDB using <tt>dbWriteTable</tt>, this is very inefficient for large files and it is better to construct the database table and read the data directly using the MonetDB console client. <a href="reading_acs3yr.txt">Here</a> is a script that reads the whole-US <a href="http://factfinder2.census.gov/faces/nav/jsf/pages/searchresults.xhtml?refresh=t">ACS 3yr person data</a>, which comes in four <tt>CSV</tt> files into a table in MonetDB.
<li> <a href="acsanalyses.R">Examples</a> of setting up and using an ACS 3-yr person file.
<li> Here is a <a href="verify.Rout">R output</a> that reproduces some of the <a href="http://www.census.gov/acs/www/Downloads/data_documentation/pums/Estimates/pums_estimates10.lst">Census Bureau totals for the 2008-2010 ACS</a>
</ul>


   Supported analyses:
   <ul>
<li> For surveys using either linearisation or replicate weights
<ul>
   <li> Means and totals with standard errors, by grouping variables
<li> Quantiles
<li> Thinned or hexagonally binned scatterplots
<li> Smoothers and density estimators
   <li> Linear regression models (with standard errors)
<li> Contingency tables
<li> Subpopulation estimates
</ul>
<li> Only with replicate weights
<ul>
<li> Quantiles with standard errors and confidence intervals
<li> Loglinear models, with Rao-Scott tests, including tests for independence in 2x2 tables
</ul>
</ul>
None of the analyses will modify any existing database table, and the R survey design objects behave as if they are passed by value, like ordinary R objects.

<p> The <strong>project summary page</strong> you can find <a href="http://<?php echo $domain; ?>/projects/<?php echo $group_name; ?>/"><strong>here</strong></a>. </p>

</body>
</html>
