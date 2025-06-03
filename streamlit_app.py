# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col, when_matched


from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title(f"Pending Orders :balloon: {st.__version__}")
st.write(
  """Pending Orders
  And if you're new to **Streamlit**, check
  out our easy-to-follow guides at
  [docs.streamlit.io](https://docs.streamlit.io).
  """
)


session = get_active_session()
pendingorders_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

if pendingorders_dataframe:
    editable_dataframe = st.data_editor(pendingorders_dataframe)
    order_status_update = st.button('Update Order Status')

    if order_status_update:
        edited_dataset = session.create_dataframe(editable_dataframe)
        
        #st.dataframe(data=my_dataframe, use_container_width=True)
        og_dataset = session.table("smoothies.public.orders")
        try:
            og_dataset.merge(edited_dataset
                     , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                     , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                    )
            st.success("Orders updated !!")
        except:
            st.write("Something Wrong !!")
else:
    st.write("There are no pending orders right now")