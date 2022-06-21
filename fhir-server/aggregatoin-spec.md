Assumptions:

- all fire path expression operate on and return collections of elements.
- all elements regardless of their type have unique identities (including simple types).

We can describe operation with signatures noting the cardinality of arguments and results, e.g,:

    count: (0..n) => 1..1
    first: (0..n) => 0..1
    where: (0..n) => 0..n
    
       
## Aggregation

Assumptions: 
 - expressions that always produce list of one element can be used as aggregating operation. 


### Simple aggregation
   
We will define a simple aggregation expression as:

    <element-selection>.<agg-operation>

- `element-selection` can be any expression fhir (including aggregations). In general, however, this is an expression of `0..n` cardinality.
- `agg-operation` can be any fhir operation of signature `(0..n) => 0..1`
   
Let's consider the following:

    - Patient:
        id: 1
        gender: 'male'
        - name:
            family: 'Smith'
            - given: 'Peter'
            - given: 'Adam'    
    - Patient:
        id: 2
        gender: 'female'
        - name:
            family: 'Brown'
            - given: 'Ann'
            - given: 'Marie'
        - name:
            family: 'White'
            - given: 'Ann'
            - given: 'Marie'
    - Patient:
        id: 3
        gender: null
        - name:
            family: 'Bond'
            - given: 'James'
        - name:
            family: 'Jones'
            - given: 'Craig'
             

Examples for `count()` operation:

    %Patient.count()
    [Patient(0), Patient(1), Patient(2)].count() == 3
    
    %Patient.gender.count()
    
    [Patient(0).gender, Patient(1).gender].count()
    ['male','female'] == 2
  
    %Patient.name.count()    
    [Patient(0).name(0), Patient(1).name(0),Patient(1).name(1), Patient(2).name(0), Patient(2).name(1)].count() == 5
  
    %Patient.name.family.count()    
    [Patient(0).name(0).family, Patient(1).name(0).family, 
        Patient(1).name(1).family, Patient(2).name(0).family, Patient(2).name(1).family].count()
    ['Smith', 'Brown', 'White', 'Bond', 'Jones'].count() == 5
 
    %Patient.name.given.count()    
     [Patient(0).name(0).given(0),Patient(0).name(0).given(1), 
        Patient(1).name(0).given(0), Patient(1).name(0).given(1), 
        Patient(1).name(1).given(0), Patient(1).name(1).given(1),
        Patient(2).name(0).given(0), Patient(2).name(1).given(0)].count()
    ['Peter', 'Adam', 'Ann', 'Marie', 'Ann', 'Marie', 'James', 'Craig'].count() == 8
 

 ### Filters
 
Filters can be applied at any level of an `element-selection`, with `where` operation to narrow down the list of elements passed to the `agg-operation`.
`where` applied at the resource level correspond to the current semantics for filters in aggregations.


Examples:

    %Patient.where(gender='male').count()
    [Patient(0))].count() == 1
    
    agg: %Patient.count()
    group: <None>
    filter: %Patient.gender='male'


This become more complex however for expression like this:

    agg: %Patient.count()
    group: <None>
    filter: [%Patient.name.family contains 'Bond', %Patient.name.given contains 'Craig']

Is that: 

    %Patient.where((name.family contains 'Bond') and (name.given contains 'Craig')).count() == 1
    
Or:

    %Patient.where(
        name.where((family contains 'Bond') and (given contains 'Craig')).empty().not()
        ).count() == 0

The explicit filter notation with `where` allows for both cases. The latter one is probably the more intuitive
representation of the complex expression above, but in general case can become quite complex with multiple levels
of `where` nesting.
    
The actual transformation can be formalized but intuitively creates conjunction of nested `where` clauses.
 
Or we can introduce a version of `and` operator that applies common prefix resolution, e.g. `%and%` at least for the 
purpose of specification (if not in the actual implementation).

Then:

    agg: %Patient.count()
    group: <None>
    filter: [%Patient.name.family contains 'Bond', %Patient.name.given contains 'Craig']

can be simply expressed as:

    %Patient.where((name.family contains 'Bond') %and% (name.given contains 'Craig')).count() == 0


Another issue is the interpretation of:

    agg: %Patient.name.count()
    group: <None>
    filter: [%Patient.name.given family 'Brown']

Should that be:

    %Patient.where(name.family contains 'Brown').name.count() == 2

or: 

    %Patient.name.where(family contains 'Brown').count() == 1

Most likely the latter, so again the filter should be applied to the longest common prefix with the aggregation expression.

But perhaps the explicit expression is better ???     
    
### Complex aggregation expressions

Aggregation expressions can be combined with operators provided that operator result is  `0..1`.
   

    %Patient.where(gender='male').count() / %Patient.count()
    [Patient(0)].count() / [Patient(0), Patient(1), Patient(2)].count()== 1/3

### Union and merge in element-selection

TBP: do they provide multiple  identities. In principle elements from merged list should be treated as separate elements
(even if they have the same ids).


## Grouping

Groupings provide the way to split elements selected for aggregation into separate groups (collections) and aggregate them independently with regard to each group. 
 
If an `element-selection` expressions returns a list of elements `ES = {e1, e2, e3, ..., en}`
Than a grouping is any expression that subsets 'ES'
In general groupings can overlap and could be multi-sets (e.g. include more that one copy of the input element).

### One dimensional groupings

In this context a grouping expression can be any expression that can be inserted as a `where` function
after element selection, noting that it needs to operate on the selected elements. 
Access to the parent element can be granted through down traversal (`$parent`).

Please note that in current implementations the uses `$parent` implicitly by resolving actual grouping expressions in the context of selected element.

So we could consider something like:

    %Patient.name.where($parent.gender = 'male').count() == 1
    %Patient.name.where($parent.gender = 'female').count() == 1
    %Patient.name.where($parent.gender.empty()).count() == 1
    
These grouping (expression) can be generated automatically from the valid values of `$parent.gender`
Or with the implicit `$parent` with the grouping expression: `%Patient.gender`.

Maybe then we could also consider the following syntax (at least here for this specification)

    %Patient.name.groupBy($parent.gender).count() == {'male':1, 'female':1, null:1}
    or with implicit $parent resolution
    %Patient.name.groupBy(%Patient.gender).count() == {'male':1, 'female':1, null:1}
    
    
Please note that there is a difference between:

    %Patient.groupBy(%Patient.gender).count() == %Patient.groupBy($this.gender).count():    
        %Patient.where(gender = 'male').count() == 1
        %Patient.where(gender = 'female').count() == 2
        %Patient.where(gender.empty()).count() == 1

and:

    %Patient.gender.groupBy(%Patient.gender).count() == %Patient.gender.groupBy($this).count():    
        %Patient.gender.where($this = 'male').count() == 1
        %Patient.gender.where($this = 'female').count() == 1

as the latter does not include the empty group (`null` is not a valid element of `%Patient.gender` but
`gender.empty()` is a possible value for `%Patient` ).

   
Or more complex example:

    %Patient.name.where(given contains 'Ann').family.groupBy($parent.given).count():
        %Patient.name.where(given contains 'Ann').family.where($parent.given contains 'Ann').count() == 2
        %Patient.name.where(given contains 'Ann').family.where($parent.given contains 'Marie').count() == 2

In this case grouping (expression) can be generated automatically from the valid values of `$parent.given` 
(for the selected elements only, e.g: `%Patient.name.where(given contains 'Ann').family`).
Here however the grouping expressions overlap.

### Filters in grouping expressions

These should have the straightforward interpretation of limiting the number of values considered for creation 
of groups. E.g.:

    %Patient.groupBy(gender).count():
        male: %Patient.where(gender = 'male').count() 
        female: %Patient.where(gender = 'female').count() 
        null: %Patient.where(gender.empty()).count() 
        
vs:

    %Patient.groupBy(gender.where(empty().not()).count():
        male: %Patient.where(gender.where(empty().not() = 'male').count() 
        female: %Patient.where(gender.where(empty().not() = 'female').count() 


### Multi-dimensional groups

This essentially comrises of two problems:

- how to generate valid combination of values for grouping expressions
- how to generate the grouping expressions themselves


E.g.:


    %Patient.where(gender='female').groupBy(name.given, name.family).count():
        (Brown, Ann):  %Patient.where(gender='female').where(name.family='Brown' %and% name.given contains 'Ann').count()
        (Brown, Marie): %Patient.where(gender='female').where(name.family='Brown' %and% name.given contains 'Marie').count()
        (White, Ann): %Patient.where(gender='female').where(name.family='White' %and% name.given contains 'Ann').count()
        (White, Marie): %Patient.where(gender='female').where(name.family='White' %and% name.given contains 'Marie').count()
    
   
    agg: %Patient.where(gender='female').count():
    group: name.given, name.family
    filter: %Patient.gender='female'

Please note the use of `%and%` above.

    %Patient.name.groupBy($parent.gender, family).count():
        ('male', 'Smith): %Patient.name.where($parent.gender = 'male' and family='Smith').count() 

    agg: %Patient.name.count()
    group: %Patient.gender, %Patient.name.family
    filter: <None>


Note this may result in some quite complex expressions that need to track the resolution of common prefixes.
Something like:

   %Patient.groupBy(gender, name.family, name.given)

will need to use:

   %Patient.where(gender = 'male' %and% name.family='Smith' %and% name.given='Adam')

### Multi-sets (bags)

The `groupBy` interpretation above (as `where` on `element-selection`) always produces a group 
a subset of the initial list of selected elements, so each element appears in each group at most once.
(although it might appear in multiple groups or not be present in some groups).

It is be possible however to envision groupings that allow an element to be in a group many times.

Let's consider:

    %Patient.where(gender='female').groupBy(name.given).count():
        %Patient.where(gender='female').where(name.given contains 'Ann').count() == 1
        %Patient.where(gender='female').where(name.given contains 'Marie').count() == 1
    
Each patient is only once here even though it's `name.given` collection contains
 'Ann' or 'Marie' multiple times (twice).

Would that be useful? Could that be achieved with switching the aggregation context?

This could possibly be usefule to compute some form of weighted averages, where mulitple entires for the same element
are desireable. E.g what is the average age of patients that underwent specific procedure.
We want to avergae over age of the procedure subject for all procedures regadless to how many patients they were applied to.

It seems that we are currently missing the way to express that. 
One way to achieve this would be to further extend `groupBy` syntax to something akin Spark SQL groupBy.  
 
    %Procedure.groupBy(type).resolve(subject).age.mean()
   
where `groupBy` would create a special form of `GroupedFhirPath`  where the context is switched from resource ID to grouping columns.

Or othwerwise that could be written as:

    %Patient.age.groupByWithMuliSet($parent.id.reverseResolve(Procedure).type).mean()
 
The former seems to be much clearer but, the question is if in can be (in general case) unambiguously expressed 
in the current paradigm, such as: 
    
    agg: %Procedure.resolve(subject).age.mean()
    group: %Procedure.type
    filter: <None>






 




