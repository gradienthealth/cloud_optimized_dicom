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
- `{series_uid}.tar`: contains all instance.dcm files for this series
- `{series_uid}/metadata.json`: contains all dicom tags for each instance, along with additional metadata
- `{series_uid}/index.sqlite`: an index used by the `Ratarmount` package [@ratarmount] 
to efficiently retrieve individual instances from the tar without indexing the whole thing
- `{series_uid}/thumbnail.{mp4|jpg}`: (optional) a small thumbnail containing each frame in the series

The overall file structure is modeled after the DICOMWEB spec, 
i.e. an instance can be found at `studies/{study_uid}/series/{series_uid}.tar://instances/{instance_uid}.dcm`.

Fetching and caching of the series tar is abstracted away from the end user in an optimal manner.
Additional utility functionality is also included, such as the ability to add custom metadata fields, generate thumbnails, 
and use a user-provided hash function to de-identify the UIDs in the URI and metadata.

![A visualization of the COD file structure](cod_filestructure.png){height="10cm"}

# Statement of need

At Gradient Health, we store over 5 petabytes dicom data and counting. 
Specifically, we have over 18M studies, broken into 67M series and hundreds of millions of instances.
We receive this data in a myriad of formats, but most commonly single frame instance-level .dcm files.

This format is sub-optimal at scale for many reasons, the most obvious of which is cost.
Training AI models - a common use case for data such as this - requries retriving every single data point.
If the data is sharded into instance-level files, this is quite expensive (see example below).

## Retrieval Cost Savings Example

Consider a scenario where you are trying to train an AI model on a dataset of 10 million CT scans.
For simplicity, let us say that each CT has 100 slices.
In order to do this training, the entire dataset must be retrieved - every single slice.
In standard (non-multiframe) dicom, each of these slices will be stored in its own `.dcm` file, for a total of 1 billion files.
At Google's standard storage rate of $0.005 / 1k GET requests [@gcs_pricing],
it would cost you $5,000 to retrieve the whole dataset.

With COD, dicom data is grouped into series-level tar files. 
Regardless of how many instances/frames are in a series, it costs a fixed 3 GET reqeusts
to retrieve a series (one each for the `tar`, the `metadata.json`, and the `index.sqlite`).

In this example, this means that instead of having to retrieve 1 billion instance files,
we instead retrieve 10 million COD objects (for a total of 30 million GETs).
With Google's pricing model this brings the total retrieval cost down to $150 - a remarkable 97% cost reduction.

Note: Cloud providers also charge by the GB for egress in addition to by request,
but the total size difference between COD and raw data storage is minimal.

## Other solutions
### Multiframe dicom files
Another possible solution to this data sharding cost issue would be to group instances by series and merge them all into series-level multiframe dicom files.

While this solution is viable, the main reason we opted to develop COD instead is data provenance. 
Manipulating raw data into a multiframe introduces another layer where something could go wrong. 
In contrast, COD does not alter the original data in any way - the philosophy being "the less you touch it, the better".

The tradeoff is that write operations are heavier and more expensive,
but the main use case for dicom is retrieval (not editing), which is why we believe this tradeoff is worth it.

Our format is also more easily extensible than a multiframe dicom, allowing for custom metadata fields
and even additional series level files like thumbnails, for which COD provides explicit support.

### Sharding (Intelerad)
It is a common usage pattern for metadata to be accessed more frequently than full resolution image data.
This presents the potential for a "sharding" solution.
Intelerad [@intelerad] is a proprietary implementation of this - 
metadata is stored in a `.dcm` file, but pixel data is stored separatly in an image file (`.jpg`, `.j2c`, etc.).

Unfortunately, when the use case is image data retrieval
Intelerad's proprietary shard format does not offer any cost savings because there is no reduction in retrieved file count.

Consider agian the Retrieval Cost Savings Example with Intelerad sharding;
instead of retrieving 1 billion `.dcm` files we would retrieve 1 billion `.jpg` files, which would have identical cost.

### Proprietary Implementations: GCloud & AWS for Healthcare
Cloud providers have recognized the demand for and created their own healthcare data storage solutions.
While elegant and easy to use, these solutions have a much higher cost than COD.
Consider Google Cloud Healthcare API's dicom pricing example [@healthcare_pricing].

Summarizing, they are quoting $6.96 per month to store 151,000 instances spread across 1,500 studies, 
each retrieved twice, with a total size of 160GB.

Specifically, $1.08 for retrieval, $4.60 for storage, and $1.28 for "ETL Operations" (transcoding the images on retrieval).

Of the studies, 1,000 are single image X rays - this would translate to 1,000 series.

The remaining 500 studies are 300-image MRIs/CTs - let us assume these constitute 500 series.

In these conditions, this same scenario in COD using Standard storage would cost $3.25

Retrieval cost: $2 * 3 * 1500 * 0.005 / 1000 \approx \$0.05$

(2 full retrievals; 3 GETs per series; 1,500 series; $0.005 per 1k standard GETs [@gcs_pricing])

Storage cost: $0.02 * 160 = \$3.20$

Transcoding cost: $0 (COD does not alter the data in any way and returns the images in their original enconding).

Furthermore, if archive storage were to be used instead of standard, COD would cost a grand total of $0.65 [@gcs_pricing]:

Retrieval cost: $2 * 3 * 1500 * 0.05 / 1000 = \$0.45$

Storage cost: $0.0012 * 160 \approx \$0.20$

### Generalized Solution: Cloud-native DFS
Another option to cut costs on dicom storage/retrieval could be to use a cloud-based distributed file system like JuiceFS [@juicefs]. 
Indeed, the idea of using cloud buckets for AI training is an up and coming technology that has been gaining traction.
The main selling point is scalability - there can be petabytes of cloud stored data, 
but it simply is not feasible to have SSDs at the petabyte level for a single GPU cluster.
So, while JuiceFS or a similar technology would indeed be an effective way to storge and retrieve large quantities of dicom files, 
COD's main advantage is its simplicity and dicom specific design.
Dicom already has the P10 format for moving things around, and COD essentially preserves the P10 format -
it only requies the data to be un-tarred, which is a very common interface that would never require driver installation or custom code handling.

COD also holds the advantage in robustness and diagnosability.
Should a file be corrupted, the format which was provided can be easily inspected within the `.tar` file. 
Furthermore, should either the `index.sqlite` or `metadata.json` become corrupted, they can be reformed from the `.tar` itself.

## Benchmarks
Below is a table outlining the performance and cost savings of COD on various test datasets.

| Dataset                          | Size (GB) | Num Files | Total Cost ($)  | $ / GB  | $ / 1k files  |
|----------------------------------|-----------|-----------|-----------------|---------|---------------|
| Emory (TODO: CITE...?)           | 2656.6    | 480,606   | 12.07           | 0.0045  | 0.0251        |
| NIH Chest Xrays [@gcp_nih_chest] | 117.7     | 112,122   | 1.81            | 0.0154  | 0.0161        |
| NLST Cancer [@nlst]              | ???       | ???       | ???             | ???     | ???           |

## When does COD become cheaper?

Initially, COD is more expensive as it requires an upfront ingestion cost.

This cost eventually pays for itself as the data is retrieved.

For the purpose of cost analysis we use the concept of a "full dataset retrieval".
This simply refers to a GET on every file in the dataset,
which is what would happen each epoch of an ML training run, for example.

We can define the number of retrievals required to break even as $b = \frac{c_i}{c_r - c_c}$,
where $c_i, c_r$ and $c_c$ are the costs of COD ingestion, raw retrieval, and COD retrieval respectively.

This can be expanded to 
$$b = \frac{i s n}{(c_g s n) - (3 c_g s)}$$
where
$i$ is the COD ingestion cost per file,
$s$ is the number of series in the dataset,
$n$ is the average number of instances per series in the dataset, and
$c_g$ is the cost per GET request.

Note the constant 3 - this is because getting a series with COD actually constitutes three get requests at the series level:
One each for the `tar`, the `metadata.json`, and the `index.sqlite`.

We can simplify this equation to
$$b = \frac{i n}{c_g(n - 3)} = \frac{i}{c_g(1 - \frac{3}{n})} \label{breakeven}$$

Based on our benchmarks, we estimate $i \approx 0.00002$ (TODO update when NLST is done), 

Using this, we can compute the number of "full dataset retrievals" 
required to break even on COD for each GCloud storage mode:

+-------------------+------------------+----------+----------+----------+----------+----------+-----------+
| Storage Class     | Cost per 1k GETs | Break-even Retrieval Count by Avg # Instances / Series           |
+-------------------+------------------+----------+----------+----------+----------+----------+-----------+
|                   |                  | 0-3      | 5        | 10       | 20       | 100      | 1000      |
+:=================:+:================:+:========:+:========:+:========:+:========:+:========:+:=========:+
| Standard          | 0.005            | N/A      | 10.31    | 5.89     | 4.85     | 4.25     | 4.14      |
+-------------------+------------------+----------+----------+----------+----------+----------+-----------+
| Nearline          | 0.01             | N/A      | 5.16     | 2.95     | 2.43     | 2.13     | 2.07      |
+-------------------+------------------+----------+----------+----------+----------+----------+-----------+
| Coldline          | 0.02             | N/A      | 2.58     | 1.47     | 1.21     | 1.06     | 1.03      |
+-------------------+------------------+----------+----------+----------+----------+----------+-----------+
| Archive           | 0.05             | N/A      | 1.03     | 0.59     | 0.49     | 0.43     | 0.41      |
+-------------------+------------------+----------+----------+----------+----------+----------+-----------+

Note: For 3 or fewer average instances per series, COD will never break even - because COD costs 3 GETs per series,
raw retrieval of series with 3 or fewer instances is actually cheaper than COD retrieval.

# Future Directions
- A high-throughput pytorch wrapper that is able to load COD data and use it very directly
- support for additional cloud providers besides Google