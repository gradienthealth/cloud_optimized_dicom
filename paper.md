---
title: 'Cloud Optimized Dicom: A Python package for efficiently ingesting & manipulating medical imagery in cloud storage'
tags:
  - Python
  - dicom
  - google cloud
  - COD
  - optimization
authors:
  - name: Cal J. Nightingale
    orcid: 0009-0005-5711-1793
    affiliation: 1
  - name: Ouwen Huang
    affiliation: 1
affiliations:
 - name: Gradient Health, United States
   index: 1
date: 31 July 2025
bibliography: paper.bib
---

# Summary

The Cloud Optimized Dicom package (or COD) provides a framewowrk for storing, manipulating,
and retriving dicom files in the cloud in a cost-optimal way.
We propose a novel data structure for storing dicom data at scale, consisting of the following series-level files:
- {series_uid}.tar: contains all instance.dcm files for this series
- {series_uid}/metadata.json: contains all dicom tags for each instance, along with additional metadata
- {series_uid}/index.sqlite: an index used by the `Ratarmount` package [@ratarmount] 
to efficiently retrieve individual instances from the tar without indexing the whole thing
- (OPTIONAL) {series_uid}/thumbnail.{mp4|jpg}: a 100x100px thumbnail containing each frame in the series
These files are located at a URI conformant to the DICOMWEB spec, e.g. "{my-bucket}/studies/{study_uid}/series/{series_uid}.tar".
Fetching and caching of the series tar is abstracted away from the end user in an optimal manner.
Additional utility functionality is also included, such as the ability to add custom metadata fields, generate thumbnails, 
and use a user-provided hash function to de-identify the UIDs in the URI and metadata.

# Statement of need

At Gradient Health, we store over 5 petabytes dicom data and counting. 
Specifically, we have over 18M studies, broken into 67M series and ???M (TODO: estimate) or more instances.
Because GCP and other cloud providers bill by the GET request, if we stored this data as raw dicom at the instance level,
it would cost us (TODO: some math to show a big number here) to retrieve our entire dataset.
We, along with other data providers/hospitals/etc. who store large quanitites of DICOM data, 
were in need of some way to reduce this cost.

With COD, we were able to reduce our montly cloud storage costs by __% (TODO: estimate or compute).

# Citations

Citations to entries in paper.bib should be in
[rMarkdown](http://rmarkdown.rstudio.com/authoring_bibliographies_and_citations.html)
format.

If you want to cite a software repository URL (e.g. something on GitHub without a preferred
citation) then you can do it with the example BibTeX entry below for @fidgit.

For a quick reference, the following citation commands can be used:
- `@author:2001`  ->  "Author et al. (2001)"
- `[@author:2001]` -> "(Author et al., 2001)"
- `[@author1:2001; @author2:2001]` -> "(Author1 et al., 2001; Author2 et al., 2002)"

# Figures

Figures can be included like this:
![Caption for example figure.\label{fig:example}](figure.png)
and referenced from text using \autoref{fig:example}.

Figure sizes can be customized by adding an optional second parameter:
![Caption for example figure.](figure.png){ width=20% }

# Acknowledgements

We acknowledge contributions from Brigitta Sipocz, Syrtis Major, and Semyeong
Oh, and support from Kathryn Johnston during the genesis of this project.

# References