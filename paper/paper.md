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
Specifically, we have over 18M studies, broken into 67M series and hundreds of millions of instances.
We receive this data in a myriad of formats, but most commonly single frame instance-level .dcm files.

This format is sub-optimal at scale for many reasons, the most obvious of which is cost.
Training AI models - a common use case for data such as this - requries retriving every single data point.
If the data is sharded into instance-level files, this is quite expensive.

## Retrieval Cost Savings Example

Consider a worst case scenario: your dataset has 10 million studies, each of which has 10 series on average, 
which in turn have 10 instances on average (for a total of 1B instances). 
At a rate of $0.005 / 1k GET requests (which is what Google charges), it would cost you $5,000 to retrieve the whole dataset.

With COD, dicom data is grouped into series-level tar files. 
Say that series A has instances 1, 2, and 3.
Without COD, to retrieve series A it would cost you 3 GET requests - one for each instance.
With COD, it only costs 1 GET, as A contains all three instances in a single tarfile.

Therefore, COD reduces the cost to retrieve a dicom dataset by a factor of x, 
where x is the average number of instances per series in the dataset.

Returning to our 1B instance example, COD gives a tenfold reduction in total retrieval cost, bringing the price down to $500 to retrieve the whole dataset.

## Why not just convert to multiframe?
Another possible solution to this data sharding cost issue would be to group instances by series and merge them all into a single series-level multiframe dicom file.

While this solution is viable, the main reason we opted to develop COD instead is data providence. 
Manipulating raw data into a multiframe introduces another layer where something could go wrong. 
In contrast, COD does not alter the original data in any way - the philosophy being "the less you touch it, the better.

(clearly show why COD is a good thing)
1. transition costs for storage - if you store a bunch of tiny files it is quite expensive. To transition them would cost more than the storage. BREAKDOWN (for example w CT images), concrete numbers
2. minimal data corruption - why not just convert to multiframes? answer: bc multiframes can have errors, hospitals themselves can provide corrupt data... the less you touch it, the better the data providence.
TRADEOFF: we do have heavier writes, BUT dicom data is not really being updated that much (or shouldnt be). Main use case is retrieval.

add a figure explaining the format?

idea of extending cod w/ additional data (like thumbnails)

cite: proprietary implementations like intelerad (shards the data) - identify limitation of data corruption risk due to manipulation.
cite: google cloud for healthcare/amazon as prop. implementation - identify limitation of high cost. What's the compute cost to bytes processed metric?

Cost of COD: compute cost to run it (get numbers for this - bytes processed metric), and after there's the cost of transitioning storage (standard -> archive), and finally the actual storage cost.

Show COD is vastly cheaper as an option when it comes to the alts like amazon/google.

Company that does something similar: juiceFS: https://juicefs.com/en/. What COD is but for general data. Refernece it and say the idea of using cloud buckets for AI training is the up and coming way. Why do that? its because they scale. You can have petabytes of cloud stored data but you're not gonna have SSDs at the petabyte level for a single GPU cluster. This is an idea that has been gaining traction. Why not just use JuiceFS? Its about having a simple to use and export dicom. Dicom already has the P10 format for moving things around, we basically keep the P10 format and it only requies the data to be un-tarred which is a very common interface (would never require driver install or custom code handling for example.)

## Benchmarks
Below is a table outlining the performance and cost savings of COD on various test datasets.

| Dataset         | Size (GB) | Num Files | Total Cost ($)  | $ / GB  | $ / 1k files  |
|-----------------|-----------|-----------|-----------------|---------|---------------|
| Emory           | 2656.6    | 480,606   | 12.07           | 0.0045  | 0.0251        |
| NIH Chest Xrays | 117.7     | 112,122   | 1.81            | 0.0154  | 0.0161        |
| NLST Cancer     | ???       | ???       | ???             | ???     | ???           |


demonstrate cost of COD conversion on the following datasets:
- laplace-open-embed (emory)
- nih chest xrays: https://cloud.google.com/healthcare-api/docs/resources/public-datasets/nih-chest
- nlst cancer: https://cdas.cancer.gov/nlst/

show cost of: cod ingetstion + thumbnail gen, storage cost post cod.
TABLE SCAN COST: frames perspective = 4B frames, series = 55M series for example. 
Include storage cost pre cod?

## When does COD become cheaper?

Initially, COD is more expensive as it requires an upfront ingestion cost.

This cost eventually pays for itself as the data is retrieved.

The equation below describes a cost analysis:

$$b = \frac{c_f}{c_g (1 - \frac{1}{n})}$$

Where $b$ is the number of full dataset retrievals required to break even on COD, 
$c_f$ is the COD ingestion cost per file, 
$c_g$ is the cost per GET request, and 
$n$ is the average number of instances per series in the dataset

If we take the limit of this equation as $n$ approaches infinity, we see that this equation converges towards $\frac{c_f}{c_g}$

From GCloud's stated rate of $0.005 per 1,000 GET requests, 
and the average ingestion cost per file of roughly $0.00002 based on our benchmarks (TODO update when NLST is done), 
we can compute that the break even point converges to roughly 4 full dataset retrievals.

So, in the general case, we can say that COD pays for itself after roughly 5 dataset retrievals.

# Future Directions
- data loader (ARPA-H?). if you want to actually train AI on COD data, it would suck to take COD and reformat it to another format that training can actually use. Showcase a pytorch wrapper that is able to laod the data and use it very quickly (high throughput)
- support additional cloud providers
- pixel sharding?

# Acknowledgements

TODO

# References

TODO