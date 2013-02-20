
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

  <p> This project used to have two packages. It now just has  <a href="http://r-forge.r-project.org/R/?group_id=1484">one</a>: sqlsurvey, for analysis of large surveys. You also need <a href="https://r-forge.r-project.org/R/?group_id=1534">MonetDB.R</a>, which replaces my JDBC-based interface. There are other MonetDB to R interfaces, but they won't work with the sqlsurvey package, because we've had to extend the R-DBI interface to handle concurrency problems from garbage collection. 

   <p> Both packages require <a href="http://www.monetdb.org/Downloads?x=144&y=18">MonetDB</a>, so installation is more complicated than just installing an R package. 
 Under Windows it is important to use a 64-bit version of MonetDB to allow creation of large databases. </p>


Examples (small enough to play with):
<ul>
<li> ACS 3-year data for Alabama (47k records, replicate weights): <a href="ss10pal.csv">CSV data</a>, <a href="acs-example.R">R script</a>
<li> Some blood pressure data from NHANES (18k records, linearisation variances):  <a href="nhanesbp.rda">.RDA file of data</a>, <a href="nhanes-example.R">R script</a>.
</ul>

 Useful notes:
<ul>
<li> Some <a href="installation.txt">installation notes</a>.  If anyone wants to write better ones, I will put them up. 
   <li> While it is possible to read data into R and then save into MonetDB using <tt>dbWriteTable</tt>, this is very inefficient for large files and it is better to construct the database table and read the data directly using the MonetDB console client. <a href="reading_acs3yr.txt">Here</a> is a script that reads the whole-US <a href="http://factfinder2.census.gov/faces/nav/jsf/pages/searchresults.xhtml?refresh=t">ACS 3yr person data</a>, which comes in four <tt>CSV</tt> files, into a table in MonetDB.
<li> <a href="acsanalyses.R">Examples</a> of setting up and using an ACS 3-yr person file.
<li> Here is a <a href="verify.Rout">R transcript</a> that reproduces some of the <a href="http://www.census.gov/acs/www/Downloads/data_documentation/pums/Estimates/pums_estimates10.lst">Census Bureau totals for the 2008-2010 ACS</a>
</ul>

   Supported analyses:
   <ul>
<li> For surveys using either linearisation or replicate weights
<ul>
   <li> Means (<tt>svymean</tt>) and totals (<tt>svytotal</tt>) with standard errors, by grouping variables
<li> Quantiles (<tt>svyquantile</tt>)
<li> Thinned or hexagonally binned scatterplots (<tt>svyplot</tt>)
<li> Smoothers and density estimators (<tt>svysmooth</tt>)
   <li> Linear regression models (with standard errors) (<tt>svylm</tt>)
<li> Contingency tables (<tt>svytable</tt>)
<li> Subpopulation estimates (<tt>subset</tt>)
</ul>
<li> Only with replicate weights
<ul>
<li> Quantiles with standard errors and confidence intervals(<tt>svyquantile</tt>)
<li> Loglinear models, with Rao-Scott tests, including tests for independence in 2x2 tables (<tt>svyloglin</tt>, <tt>svychisq</tt>)
</ul>
</ul>
<p>
None of the analyses will modify any existing database table, and the R survey design objects behave as if they are passed by value, like ordinary R objects. Temporary tables are automatically dropped when the R objects referring to them are garbage-collected. The basic design ideas for the package were described in a<a href="http://www.r-project.org/conferences/useR-2007/program/presentations/lumley.pdf">presentation</a> at UseR 2007, but were not developed further because of lack of demand.  The American Community Survey and some medical-record surveys such as the Nationwide Inpatient Sample do represent a real need, so the project has been restarted.  MonetDB turns out to be much faster than SQLite for this sort of analysis, and interactive analysis of millions of records on an ordinary desktop is quite feasible. </p>

<p> The <strong>project summary page</strong> is <a href="http://<?php echo $domain; ?>/projects/<?php echo $group_name; ?>/"><strong>here</strong></a>. </p>

</body>
</html>
